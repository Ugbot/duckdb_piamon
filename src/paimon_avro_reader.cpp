#include "paimon_avro_reader.hpp"
#include "paimon_avro_schema.hpp"
#include "paimon_metadata.hpp" // YyjsonDocDeleter
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
// Binary decoder
//===--------------------------------------------------------------------===//
struct ByteCursor {
	const_data_ptr_t p;
	const_data_ptr_t end;
	// Bytes remaining in the buffer. The (p <= end) invariant holds for every cursor we construct, so
	// this subtraction never underflows — unlike `p + n > end`, which is UB when `n` is attacker-large.
	idx_t Remaining() const {
		D_ASSERT(p <= end);
		return (idx_t)(end - p);
	}
	void Check(idx_t n) const {
		if (n > Remaining()) {
			throw IOException("Avro decode overran block buffer");
		}
	}
};

// Validate a length/count decoded from the (untrusted) stream before it is used to index, copy, or
// allocate: it must be non-negative and cannot exceed the bytes left in the buffer. A length larger
// than the remaining input is always malformed, and an element count cannot exceed the byte count
// because every element occupies at least one byte (the manifest/data schemas have no zero-width
// elements). This is a hard runtime check, not a D_ASSERT — malformed files reach this in release.
idx_t CheckedLength(const ByteCursor &c, int64_t len, const char *what) {
	if (len < 0 || (uint64_t)len > c.Remaining()) {
		throw IOException(string("Avro ") + what + " length/count out of range (corrupt file)");
	}
	return (idx_t)len;
}

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
		idx_t len = CheckedLength(c, DecodeLong(c), "bytes");
		Value v = Value::BLOB(c.p, len);
		c.p += len;
		return v;
	}
	case AvroKind::STRING: {
		idx_t len = CheckedLength(c, DecodeLong(c), "string");
		string s((const char *)c.p, len);
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
		// Avro arrays are a sequence of blocks terminated by a zero count. Each block-count read
		// consumes at least one byte and CheckedLength bounds the count by the bytes left, so the
		// loop is bounded by the buffer size and cannot spin or over-allocate on malformed input.
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
			idx_t n = CheckedLength(c, count, "array block");
			for (idx_t i = 0; i < n; i++) {
				items.push_back(DecodeValue(c, *type.items));
			}
		}
		auto child_type = items.empty() ? LogicalType::VARCHAR : items[0].type();
		return Value::LIST(child_type, std::move(items));
	}
	case AvroKind::MAP: {
		// Decode into a LIST of STRUCT(key,value) — not used by the manifest parsers, but consumed.
		// Bounded for the same reason as ARRAY above.
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
			idx_t n = CheckedLength(c, count, "map block");
			for (idx_t i = 0; i < n; i++) {
				idx_t klen = CheckedLength(c, DecodeLong(c), "map key");
				string key((const char *)c.p, klen);
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
		idx_t meta_count = CheckedLength(c, count, "file-metadata block");
		for (idx_t i = 0; i < meta_count; i++) {
			idx_t klen = CheckedLength(c, DecodeLong(c), "metadata key");
			string key((const char *)c.p, klen);
			c.p += klen;
			idx_t vlen = CheckedLength(c, DecodeLong(c), "metadata value");
			string val((const char *)c.p, vlen);
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
	AvroSchemaParser sp;
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
		idx_t block_bytes = CheckedLength(c, DecodeLong(c), "data block");
		const_data_ptr_t block = c.p;
		c.p += block_bytes;

		string decompressed;
		const_data_ptr_t data_ptr;
		idx_t data_len;
		if (codec == "null") {
			data_ptr = block;
			data_len = block_bytes;
		} else { // zstandard — stream-decompress (Avro frames may omit the content size header)
			auto ds = duckdb_zstd::ZSTD_createDStream();
			if (!ds) {
				throw IOException("Failed to create zstd decompression stream");
			}
			duckdb_zstd::ZSTD_initDStream(ds);
			duckdb_zstd::ZSTD_inBuffer in_buf {block, (size_t)block_bytes, 0};
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
		// A record occupies at least one byte, so a block cannot contain more records than it has
		// (decompressed) bytes; reject an obj_count that exceeds that to bound the loop on bad input.
		idx_t records = CheckedLength(bc, obj_count, "data block record count");
		for (idx_t i = 0; i < records; i++) {
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
