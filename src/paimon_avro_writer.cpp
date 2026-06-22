#include "paimon_avro_writer.hpp"
#include "paimon_avro_schema.hpp"
#include "paimon_metadata.hpp" // YyjsonDocDeleter

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

#include <cstring>

namespace duckdb {

namespace {

void EncodeLong(string &out, int64_t value) {
	uint64_t zz = ((uint64_t)value << 1) ^ (uint64_t)(value >> 63); // zigzag
	while (zz & ~0x7FULL) {
		out.push_back((char)((zz & 0x7F) | 0x80));
		zz >>= 7;
	}
	out.push_back((char)(zz & 0x7F));
}

void EncodeBytes(string &out, const string &bytes) {
	EncodeLong(out, (int64_t)bytes.size());
	out.append(bytes);
}

void EncodeValue(string &out, const AvroType &type, const Value &value);

void EncodeRecord(string &out, const AvroType &type, const Value &value) {
	auto &children = StructValue::GetChildren(value);
	if (children.size() != type.fields.size()) {
		throw IOException("Avro record arity mismatch when writing manifest");
	}
	for (idx_t i = 0; i < type.fields.size(); i++) {
		EncodeValue(out, *type.fields[i].second, children[i]);
	}
}

void EncodeValue(string &out, const AvroType &type, const Value &value) {
	switch (type.kind) {
	case AvroKind::NUL:
		return;
	case AvroKind::BOOL:
		out.push_back(value.GetValue<bool>() ? 1 : 0);
		return;
	case AvroKind::INT:
		EncodeLong(out, (int64_t)value.GetValue<int32_t>());
		return;
	case AvroKind::LONG:
		EncodeLong(out, value.GetValue<int64_t>());
		return;
	case AvroKind::FLOAT: {
		float f = value.GetValue<float>();
		out.append((const char *)&f, 4);
		return;
	}
	case AvroKind::DOUBLE: {
		double d = value.GetValue<double>();
		out.append((const char *)&d, 8);
		return;
	}
	case AvroKind::STRING:
		EncodeBytes(out, value.ToString());
		return;
	case AvroKind::BYTES:
		EncodeBytes(out, StringValue::Get(value));
		return;
	case AvroKind::FIXED: {
		auto s = StringValue::Get(value);
		s.resize(type.fixed_size, '\0');
		out.append(s.data(), type.fixed_size);
		return;
	}
	case AvroKind::ENUM:
		EncodeLong(out, (int64_t)value.GetValue<int32_t>());
		return;
	case AvroKind::RECORD:
		EncodeRecord(out, type, value);
		return;
	case AvroKind::UNION: {
		// Choose the null branch for NULL values, else the first non-null branch.
		idx_t null_idx = type.branches.size();
		idx_t value_idx = type.branches.size();
		for (idx_t i = 0; i < type.branches.size(); i++) {
			if (type.branches[i]->kind == AvroKind::NUL) {
				null_idx = i;
			} else if (value_idx == type.branches.size()) {
				value_idx = i;
			}
		}
		if (value.IsNull()) {
			if (null_idx == type.branches.size()) {
				throw IOException("NULL value for a non-nullable Avro union");
			}
			EncodeLong(out, (int64_t)null_idx);
		} else {
			EncodeLong(out, (int64_t)value_idx);
			EncodeValue(out, *type.branches[value_idx], value);
		}
		return;
	}
	case AvroKind::ARRAY: {
		auto &items = ListValue::GetChildren(value);
		if (!items.empty()) {
			EncodeLong(out, (int64_t)items.size());
			for (auto &item : items) {
				EncodeValue(out, *type.items, item);
			}
		}
		EncodeLong(out, 0); // end-of-array block
		return;
	}
	default:
		throw IOException("Unsupported Avro type when writing manifest");
	}
}

} // namespace

void PaimonAvroWriter::WriteFile(ClientContext &context, const string &path, const string &schema_json,
                                 const vector<vector<Value>> &rows) {
	// Parse the schema (top-level record).
	auto doc = unique_ptr<yyjson_doc, YyjsonDocDeleter>(yyjson_read(schema_json.c_str(), schema_json.size(), 0));
	if (!doc) {
		throw IOException("Failed to parse Avro write schema JSON");
	}
	AvroSchemaParser sp;
	auto root = sp.Parse(yyjson_doc_get_root(doc.get()));
	if (root->kind != AvroKind::RECORD) {
		throw IOException("Avro write schema is not a record");
	}

	string out;
	out.append("Obj\x01", 4);

	// File metadata map: { avro.schema -> schema_json, avro.codec -> "null" }.
	EncodeLong(out, 2);
	EncodeBytes(out, "avro.schema");
	EncodeBytes(out, schema_json);
	EncodeBytes(out, "avro.codec");
	EncodeBytes(out, "null");
	EncodeLong(out, 0); // end of map

	// 16-byte sync marker (fixed; only needs to be self-consistent within the file).
	const char sync[16] = {'p', 'a', 'i', 'm', 'o', 'n', 'd', 'b', 'a', 'v', 'r', 'o', 's', 'y', 'n', 'c'};
	out.append(sync, 16);

	// Single data block: object count, byte length, encoded records, sync.
	string block;
	for (auto &row : rows) {
		// Every row must supply exactly one value per schema field. Silently padding a short row with
		// NULLs would write a corrupt manifest to disk, so a mismatch is a hard error (caller bug).
		if (row.size() != root->fields.size()) {
			throw IOException("Avro write row has " + std::to_string(row.size()) + " values but the schema has " +
			                  std::to_string(root->fields.size()) + " fields");
		}
		auto rec = Value::STRUCT([&]() {
			child_list_t<Value> kv;
			for (idx_t i = 0; i < root->fields.size(); i++) {
				kv.emplace_back(root->fields[i].first, row[i]);
			}
			return kv;
		}());
		EncodeRecord(block, *root, rec);
	}
	EncodeLong(out, (int64_t)rows.size());
	EncodeLong(out, (int64_t)block.size());
	out.append(block);
	out.append(sync, 16);

	auto &fs = FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
	handle->Write((void *)out.data(), out.size());
}

} // namespace duckdb
