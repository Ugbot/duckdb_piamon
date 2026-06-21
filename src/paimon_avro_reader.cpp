#include "paimon_avro_reader.hpp"
#include "paimon_metadata.hpp" // yyjson + YyjsonDocDeleter
#include "iceberg_utils.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

#include "zstd.h"

#include <cstring>
#include <memory>

namespace duckdb {

namespace {

//===--------------------------------------------------------------------===//
// Avro schema tree
//===--------------------------------------------------------------------===//
enum class AvroKind { NUL, BOOL, INT, LONG, FLOAT, DOUBLE, BYTES, STRING, RECORD, ARRAY, MAP, UNION, FIXED, ENUM };

struct AvroType {
	AvroKind kind;
	vector<std::pair<string, shared_ptr<AvroType>>> fields; // RECORD
	shared_ptr<AvroType> items;                             // ARRAY / MAP value
	vector<shared_ptr<AvroType>> branches;                  // UNION
	idx_t fixed_size = 0;                                   // FIXED
};

// A named-type registry so schema references (a bare name reused later) resolve.
struct SchemaParser {
	case_insensitive_map_t<shared_ptr<AvroType>> named;

	shared_ptr<AvroType> ParsePrimitive(const string &name) {
		auto t = make_shared_ptr<AvroType>();
		if (name == "null") {
			t->kind = AvroKind::NUL;
		} else if (name == "boolean") {
			t->kind = AvroKind::BOOL;
		} else if (name == "int") {
			t->kind = AvroKind::INT;
		} else if (name == "long") {
			t->kind = AvroKind::LONG;
		} else if (name == "float") {
			t->kind = AvroKind::FLOAT;
		} else if (name == "double") {
			t->kind = AvroKind::DOUBLE;
		} else if (name == "bytes") {
			t->kind = AvroKind::BYTES;
		} else if (name == "string") {
			t->kind = AvroKind::STRING;
		} else {
			return nullptr; // not a primitive — a named reference
		}
		return t;
	}

	shared_ptr<AvroType> Parse(yyjson_val *v) {
		if (yyjson_is_str(v)) {
			string name = yyjson_get_str(v);
			auto prim = ParsePrimitive(name);
			if (prim) {
				return prim;
			}
			auto it = named.find(name);
			if (it != named.end()) {
				return it->second;
			}
			throw IOException("Unknown Avro type reference: " + name);
		}
		if (yyjson_is_arr(v)) {
			// Union
			auto t = make_shared_ptr<AvroType>();
			t->kind = AvroKind::UNION;
			size_t idx, max;
			yyjson_val *branch;
			yyjson_arr_foreach(v, idx, max, branch) {
				t->branches.push_back(Parse(branch));
			}
			return t;
		}
		if (!yyjson_is_obj(v)) {
			throw IOException("Malformed Avro schema node");
		}
		auto type_val = yyjson_obj_get(v, "type");
		string type_str = type_val && yyjson_is_str(type_val) ? yyjson_get_str(type_val) : "";
		if (type_str == "record") {
			auto t = make_shared_ptr<AvroType>();
			t->kind = AvroKind::RECORD;
			auto name_val = yyjson_obj_get(v, "name");
			if (name_val && yyjson_is_str(name_val)) {
				named[yyjson_get_str(name_val)] = t; // register before parsing fields (recursion)
			}
			auto fields = yyjson_obj_get(v, "fields");
			if (fields && yyjson_is_arr(fields)) {
				size_t idx, max;
				yyjson_val *f;
				yyjson_arr_foreach(fields, idx, max, f) {
					auto fname = yyjson_obj_get(f, "name");
					auto ftype = yyjson_obj_get(f, "type");
					t->fields.emplace_back(fname && yyjson_is_str(fname) ? yyjson_get_str(fname) : "", Parse(ftype));
				}
			}
			return t;
		}
		if (type_str == "array") {
			auto t = make_shared_ptr<AvroType>();
			t->kind = AvroKind::ARRAY;
			t->items = Parse(yyjson_obj_get(v, "items"));
			return t;
		}
		if (type_str == "map") {
			auto t = make_shared_ptr<AvroType>();
			t->kind = AvroKind::MAP;
			t->items = Parse(yyjson_obj_get(v, "values"));
			return t;
		}
		if (type_str == "fixed") {
			auto t = make_shared_ptr<AvroType>();
			t->kind = AvroKind::FIXED;
			auto sz = yyjson_obj_get(v, "size");
			t->fixed_size = sz && yyjson_is_int(sz) ? (idx_t)yyjson_get_int(sz) : 0;
			auto name_val = yyjson_obj_get(v, "name");
			if (name_val && yyjson_is_str(name_val)) {
				named[yyjson_get_str(name_val)] = t;
			}
			return t;
		}
		if (type_str == "enum") {
			auto t = make_shared_ptr<AvroType>();
			t->kind = AvroKind::ENUM;
			auto name_val = yyjson_obj_get(v, "name");
			if (name_val && yyjson_is_str(name_val)) {
				named[yyjson_get_str(name_val)] = t;
			}
			return t;
		}
		// {"type":"int"|"long"|...} possibly with logicalType — treat by the primitive.
		auto prim = ParsePrimitive(type_str);
		if (prim) {
			return prim;
		}
		throw IOException("Unsupported Avro schema type: " + type_str);
	}
};

//===--------------------------------------------------------------------===//
// Binary decoder
//===--------------------------------------------------------------------===//
struct ByteCursor {
	const_data_ptr_t p;
	const_data_ptr_t end;
	void Check(idx_t n) const {
		if (p + n > end) {
			throw IOException("Avro decode overran block buffer");
		}
	}
};

int64_t DecodeLong(ByteCursor &c) {
	uint64_t v = 0;
	int shift = 0;
	while (true) {
		c.Check(1);
		uint8_t b = *c.p++;
		v |= (uint64_t)(b & 0x7F) << shift;
		if (!(b & 0x80)) {
			break;
		}
		shift += 7;
		if (shift > 63) {
			throw IOException("Avro varint too long");
		}
	}
	// zigzag decode
	return (int64_t)((v >> 1) ^ (~(v & 1) + 1));
}

Value DecodeValue(ByteCursor &c, const AvroType &type);

Value DecodeRecord(ByteCursor &c, const AvroType &type) {
	child_list_t<Value> children;
	children.reserve(type.fields.size());
	for (auto &f : type.fields) {
		children.emplace_back(f.first, DecodeValue(c, *f.second));
	}
	return Value::STRUCT(std::move(children));
}

Value DecodeValue(ByteCursor &c, const AvroType &type) {
	switch (type.kind) {
	case AvroKind::NUL:
		return Value();
	case AvroKind::BOOL: {
		c.Check(1);
		return Value::BOOLEAN(*c.p++ != 0);
	}
	case AvroKind::INT:
		return Value::INTEGER((int32_t)DecodeLong(c));
	case AvroKind::LONG:
		return Value::BIGINT(DecodeLong(c));
	case AvroKind::FLOAT: {
		c.Check(4);
		float f;
		memcpy(&f, c.p, 4);
		c.p += 4;
		return Value::FLOAT(f);
	}
	case AvroKind::DOUBLE: {
		c.Check(8);
		double d;
		memcpy(&d, c.p, 8);
		c.p += 8;
		return Value::DOUBLE(d);
	}
	case AvroKind::BYTES: {
		int64_t len = DecodeLong(c);
		c.Check((idx_t)len);
		Value v = Value::BLOB(c.p, (idx_t)len);
		c.p += len;
		return v;
	}
	case AvroKind::STRING: {
		int64_t len = DecodeLong(c);
		c.Check((idx_t)len);
		string s((const char *)c.p, (idx_t)len);
		c.p += len;
		return Value(s);
	}
	case AvroKind::FIXED: {
		c.Check(type.fixed_size);
		Value v = Value::BLOB(c.p, type.fixed_size);
		c.p += type.fixed_size;
		return v;
	}
	case AvroKind::ENUM:
		return Value::INTEGER((int32_t)DecodeLong(c));
	case AvroKind::RECORD:
		return DecodeRecord(c, type);
	case AvroKind::UNION: {
		int64_t branch = DecodeLong(c);
		if (branch < 0 || (idx_t)branch >= type.branches.size()) {
			throw IOException("Avro union branch out of range");
		}
		return DecodeValue(c, *type.branches[branch]);
	}
	case AvroKind::ARRAY: {
		vector<Value> items;
		while (true) {
			int64_t count = DecodeLong(c);
			if (count == 0) {
				break;
			}
			if (count < 0) {
				count = -count;
				DecodeLong(c); // block byte size (ignored)
			}
			for (int64_t i = 0; i < count; i++) {
				items.push_back(DecodeValue(c, *type.items));
			}
		}
		auto child_type = items.empty() ? LogicalType::VARCHAR : items[0].type();
		return Value::LIST(child_type, std::move(items));
	}
	case AvroKind::MAP: {
		// Decode into a LIST of STRUCT(key,value) — not used by the manifest parsers, but consumed.
		vector<Value> entries;
		while (true) {
			int64_t count = DecodeLong(c);
			if (count == 0) {
				break;
			}
			if (count < 0) {
				count = -count;
				DecodeLong(c);
			}
			for (int64_t i = 0; i < count; i++) {
				int64_t klen = DecodeLong(c);
				c.Check((idx_t)klen);
				string key((const char *)c.p, (idx_t)klen);
				c.p += klen;
				child_list_t<Value> kv;
				kv.emplace_back("key", Value(key));
				kv.emplace_back("value", DecodeValue(c, *type.items));
				entries.push_back(Value::STRUCT(std::move(kv)));
			}
		}
		auto child_type = entries.empty() ? LogicalType::VARCHAR : entries[0].type();
		return Value::LIST(child_type, std::move(entries));
	}
	default:
		throw IOException("Unhandled Avro kind");
	}
}

} // namespace

PaimonAvroReader::PaimonAvroReader(ClientContext &context, const string &path) {
	auto &fs = FileSystem::GetFileSystem(context);
	string content = IcebergUtils::FileToString(path, fs);
	const_data_ptr_t base = (const_data_ptr_t)content.data();
	idx_t size = content.size();

	if (size < 4 || memcmp(base, "Obj\x01", 4) != 0) {
		throw IOException("Not an Avro object container file: " + path);
	}
	ByteCursor c {base + 4, base + size};

	// File metadata: a map<string,bytes>.
	string schema_json;
	string codec = "null";
	while (true) {
		int64_t count = DecodeLong(c);
		if (count == 0) {
			break;
		}
		if (count < 0) {
			count = -count;
			DecodeLong(c); // block size
		}
		for (int64_t i = 0; i < count; i++) {
			int64_t klen = DecodeLong(c);
			c.Check((idx_t)klen);
			string key((const char *)c.p, (idx_t)klen);
			c.p += klen;
			int64_t vlen = DecodeLong(c);
			c.Check((idx_t)vlen);
			string val((const char *)c.p, (idx_t)vlen);
			c.p += vlen;
			if (key == "avro.schema") {
				schema_json = val;
			} else if (key == "avro.codec") {
				codec = val;
			}
		}
	}
	if (schema_json.empty()) {
		throw IOException("Avro file missing schema: " + path);
	}
	if (codec != "null" && codec != "zstandard") {
		throw IOException("Unsupported Avro codec '" + codec + "' in " + path +
		                  "' (supported: null, zstandard)");
	}

	// 16-byte sync marker after the header.
	c.Check(16);
	const_data_ptr_t sync = c.p;
	c.p += 16;

	// Parse the schema (top-level record).
	auto doc = unique_ptr<yyjson_doc, YyjsonDocDeleter>(yyjson_read(schema_json.c_str(), schema_json.size(), 0));
	if (!doc) {
		throw IOException("Failed to parse Avro schema JSON");
	}
	SchemaParser sp;
	auto root_type = sp.Parse(yyjson_doc_get_root(doc.get()));
	if (root_type->kind != AvroKind::RECORD) {
		throw IOException("Avro top-level schema is not a record");
	}
	for (auto &f : root_type->fields) {
		column_names.push_back(f.first);
	}

	// Data blocks.
	while (c.p < c.end) {
		int64_t obj_count = DecodeLong(c);
		int64_t block_len = DecodeLong(c);
		c.Check((idx_t)block_len);
		const_data_ptr_t block = c.p;
		c.p += block_len;

		string decompressed;
		const_data_ptr_t data_ptr;
		idx_t data_len;
		if (codec == "null") {
			data_ptr = block;
			data_len = (idx_t)block_len;
		} else { // zstandard — stream-decompress (Avro frames may omit the content size header)
			auto ds = duckdb_zstd::ZSTD_createDStream();
			if (!ds) {
				throw IOException("Failed to create zstd decompression stream");
			}
			duckdb_zstd::ZSTD_initDStream(ds);
			duckdb_zstd::ZSTD_inBuffer in_buf {block, (size_t)block_len, 0};
			vector<char> out_chunk(duckdb_zstd::ZSTD_DStreamOutSize());
			while (in_buf.pos < in_buf.size) {
				duckdb_zstd::ZSTD_outBuffer out_buf {out_chunk.data(), out_chunk.size(), 0};
				size_t ret = duckdb_zstd::ZSTD_decompressStream(ds, &out_buf, &in_buf);
				if (duckdb_zstd::ZSTD_isError(ret)) {
					duckdb_zstd::ZSTD_freeDStream(ds);
					throw IOException("zstandard decompression failed in Avro file");
				}
				decompressed.append(out_chunk.data(), out_buf.pos);
				if (ret == 0) {
					break; // a frame finished exactly
				}
			}
			duckdb_zstd::ZSTD_freeDStream(ds);
			data_ptr = (const_data_ptr_t)decompressed.data();
			data_len = (idx_t)decompressed.size();
		}

		ByteCursor bc {data_ptr, data_ptr + data_len};
		for (int64_t i = 0; i < obj_count; i++) {
			auto rec = DecodeRecord(bc, *root_type);
			auto &children = StructValue::GetChildren(rec);
			vector<Value> row;
			row.reserve(children.size());
			for (auto &child : children) {
				row.push_back(child);
			}
			rows.push_back(std::move(row));
		}

		// Each block is followed by the 16-byte sync marker.
		c.Check(16);
		if (memcmp(c.p, sync, 16) != 0) {
			throw IOException("Avro sync marker mismatch in " + path);
		}
		c.p += 16;
	}
}

} // namespace duckdb
