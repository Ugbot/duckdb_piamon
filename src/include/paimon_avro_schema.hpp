#pragma once

#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "yyjson.hpp"

using namespace duckdb_yyjson;

namespace duckdb {

//! Minimal Apache Avro schema model shared by the native manifest reader and writer.
enum class AvroKind { NUL, BOOL, INT, LONG, FLOAT, DOUBLE, BYTES, STRING, RECORD, ARRAY, MAP, UNION, FIXED, ENUM };

struct AvroType {
	AvroKind kind;
	vector<std::pair<string, shared_ptr<AvroType>>> fields; // RECORD
	shared_ptr<AvroType> items;                             // ARRAY / MAP value
	vector<shared_ptr<AvroType>> branches;                  // UNION
	idx_t fixed_size = 0;                                   // FIXED
};

//! Parses an Avro schema JSON node into an AvroType tree, resolving named-type references.
struct AvroSchemaParser {
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
			return nullptr;
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
		if (!type_val || !yyjson_is_str(type_val)) {
			throw IOException("Avro schema object missing a string 'type'");
		}
		string type_str = yyjson_get_str(type_val);
		if (type_str == "record") {
			auto t = make_shared_ptr<AvroType>();
			t->kind = AvroKind::RECORD;
			auto name_val = yyjson_obj_get(v, "name");
			if (name_val && yyjson_is_str(name_val)) {
				named[yyjson_get_str(name_val)] = t;
			}
			auto fields = yyjson_obj_get(v, "fields");
			if (fields && yyjson_is_arr(fields)) {
				size_t idx, max;
				yyjson_val *f;
				yyjson_arr_foreach(fields, idx, max, f) {
					auto fname = yyjson_obj_get(f, "name");
					auto ftype = yyjson_obj_get(f, "type");
					if (!fname || !yyjson_is_str(fname)) {
						throw IOException("Avro record field missing a string 'name'");
					}
					if (!ftype) {
						throw IOException("Avro record field '" + string(yyjson_get_str(fname)) +
						                  "' missing 'type'");
					}
					t->fields.emplace_back(yyjson_get_str(fname), Parse(ftype));
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
			int64_t fixed = sz && yyjson_is_int(sz) ? yyjson_get_int(sz) : 0;
			if (fixed < 0) {
				throw IOException("Avro fixed type has negative size");
			}
			t->fixed_size = (idx_t)fixed;
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
		auto prim = ParsePrimitive(type_str);
		if (prim) {
			return prim;
		}
		throw IOException("Unsupported Avro schema type: " + type_str);
	}
};

} // namespace duckdb
