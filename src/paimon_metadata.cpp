#include "paimon_metadata.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "iceberg_utils.hpp"

#include <fstream>
#include <sstream>
#include <regex>

namespace duckdb {

//===--------------------------------------------------------------------===//
// PaimonDataType Copy Constructor
//===--------------------------------------------------------------------===//

PaimonDataType::PaimonDataType(const PaimonDataType &other) {
	type_root = other.type_root;
	precision = other.precision;
	scale = other.scale;
	if (other.element_type) {
		element_type = make_uniq<PaimonDataType>(*other.element_type);
	}
	if (other.key_type) {
		key_type = make_uniq<PaimonDataType>(*other.key_type);
	}
	if (other.value_type) {
		value_type = make_uniq<PaimonDataType>(*other.value_type);
	}
	fields = other.fields;
}

PaimonDataType &PaimonDataType::operator=(const PaimonDataType &other) {
	if (this != &other) {
		type_root = other.type_root;
		precision = other.precision;
		scale = other.scale;
		element_type = other.element_type ? make_uniq<PaimonDataType>(*other.element_type) : nullptr;
		key_type = other.key_type ? make_uniq<PaimonDataType>(*other.key_type) : nullptr;
		value_type = other.value_type ? make_uniq<PaimonDataType>(*other.value_type) : nullptr;
		fields = other.fields;
	}
	return *this;
}

//===--------------------------------------------------------------------===//
// Paimon Type Conversion
//===--------------------------------------------------------------------===//

LogicalType PaimonTypeToLogicalType(const PaimonDataType &paimon_type) {
	switch (paimon_type.type_root) {
	case PaimonTypeRoot::BOOLEAN:
		return LogicalType::BOOLEAN;
	case PaimonTypeRoot::INT:
		return LogicalType::INTEGER;
	case PaimonTypeRoot::LONG:
		return LogicalType::BIGINT;
	case PaimonTypeRoot::FLOAT:
		return LogicalType::FLOAT;
	case PaimonTypeRoot::DOUBLE:
		return LogicalType::DOUBLE;
	case PaimonTypeRoot::STRING:
		return LogicalType::VARCHAR;
	case PaimonTypeRoot::DATE:
		return LogicalType::DATE;
	case PaimonTypeRoot::TIMESTAMP:
		return LogicalType::TIMESTAMP;
	case PaimonTypeRoot::BINARY:
		return LogicalType::BLOB;
	case PaimonTypeRoot::DECIMAL:
		if (paimon_type.precision > 0) {
			return LogicalType::DECIMAL(paimon_type.precision, paimon_type.scale >= 0 ? paimon_type.scale : 0);
		}
		return LogicalType::DECIMAL(38, 18);
	case PaimonTypeRoot::ARRAY:
		if (paimon_type.element_type) {
			return LogicalType::LIST(PaimonTypeToLogicalType(*paimon_type.element_type));
		}
		return LogicalType::LIST(LogicalType::VARCHAR);
	case PaimonTypeRoot::MAP:
		if (paimon_type.key_type && paimon_type.value_type) {
			return LogicalType::MAP(PaimonTypeToLogicalType(*paimon_type.key_type),
			                        PaimonTypeToLogicalType(*paimon_type.value_type));
		}
		return LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	case PaimonTypeRoot::STRUCT: {
		child_list_t<LogicalType> children;
		for (auto &field : paimon_type.fields) {
			children.push_back(make_pair(field.name, PaimonTypeToLogicalType(field.type)));
		}
		if (children.empty()) {
			return LogicalType::VARCHAR;
		}
		return LogicalType::STRUCT(std::move(children));
	}
	default:
		return LogicalType::VARCHAR;
	}
}

//===--------------------------------------------------------------------===//
// Schema Parsing
//===--------------------------------------------------------------------===//

// Parse a Paimon type string into a PaimonDataType
// Handles: "INT", "STRING", "DECIMAL(10,2)", "ARRAY<STRING>", "MAP<STRING, INT>", "ROW<f1 INT, f2 STRING>"
static PaimonDataType ParsePaimonTypeString(const string &type_str) {
	PaimonDataType result;
	string trimmed(type_str);
	StringUtil::Trim(trimmed);
	string upper = StringUtil::Upper(trimmed);

	if (upper == "BOOLEAN") {
		result.type_root = PaimonTypeRoot::BOOLEAN;
	} else if (upper == "TINYINT" || upper == "SMALLINT" || upper == "INT" || upper == "INTEGER") {
		result.type_root = PaimonTypeRoot::INT;
	} else if (upper == "BIGINT" || upper == "LONG") {
		result.type_root = PaimonTypeRoot::LONG;
	} else if (upper == "FLOAT") {
		result.type_root = PaimonTypeRoot::FLOAT;
	} else if (upper == "DOUBLE") {
		result.type_root = PaimonTypeRoot::DOUBLE;
	} else if (upper == "STRING" || upper == "VARCHAR" || upper.find("CHAR") == 0) {
		result.type_root = PaimonTypeRoot::STRING;
	} else if (upper == "DATE") {
		result.type_root = PaimonTypeRoot::DATE;
	} else if (upper.find("TIMESTAMP") == 0) {
		result.type_root = PaimonTypeRoot::TIMESTAMP;
	} else if (upper == "BINARY" || upper == "BYTES" || upper.find("VARBINARY") == 0) {
		result.type_root = PaimonTypeRoot::BINARY;
	} else if (upper.find("DECIMAL") == 0) {
		result.type_root = PaimonTypeRoot::DECIMAL;
		// Parse DECIMAL(precision, scale)
		auto paren_start = upper.find('(');
		if (paren_start != string::npos) {
			auto paren_end = upper.find(')');
			if (paren_end != string::npos) {
				string params = upper.substr(paren_start + 1, paren_end - paren_start - 1);
				auto comma = params.find(',');
				if (comma != string::npos) {
					string prec_str = params.substr(0, comma);
					StringUtil::Trim(prec_str);
					string scale_str = params.substr(comma + 1);
					StringUtil::Trim(scale_str);
					result.precision = std::stoi(std::string(prec_str));
					result.scale = std::stoi(std::string(scale_str));
				} else {
					string prec_str(params);
					StringUtil::Trim(prec_str);
					result.precision = std::stoi(std::string(prec_str));
					result.scale = 0;
				}
			}
		}
	} else if (upper.find("ARRAY") == 0) {
		result.type_root = PaimonTypeRoot::ARRAY;
		// Parse ARRAY<element_type>
		auto angle_start = upper.find('<');
		if (angle_start != string::npos) {
			auto angle_end = upper.rfind('>');
			if (angle_end != string::npos) {
				string element_str = type_str.substr(angle_start + 1, angle_end - angle_start - 1);
				result.element_type = make_uniq<PaimonDataType>(ParsePaimonTypeString(element_str));
			}
		}
	} else if (upper.find("MAP") == 0) {
		result.type_root = PaimonTypeRoot::MAP;
		// Parse MAP<key_type, value_type>
		auto angle_start = upper.find('<');
		if (angle_start != string::npos) {
			auto angle_end = upper.rfind('>');
			if (angle_end != string::npos) {
				string inner = type_str.substr(angle_start + 1, angle_end - angle_start - 1);
				// Find the comma separating key and value types (handle nested generics)
				int depth = 0;
				idx_t split = string::npos;
				for (idx_t i = 0; i < inner.size(); i++) {
					if (inner[i] == '<') {
						depth++;
					} else if (inner[i] == '>') {
						depth--;
					} else if (inner[i] == ',' && depth == 0) {
						split = i;
						break;
					}
				}
				if (split != string::npos) {
					result.key_type = make_uniq<PaimonDataType>(ParsePaimonTypeString(inner.substr(0, split)));
					result.value_type = make_uniq<PaimonDataType>(ParsePaimonTypeString(inner.substr(split + 1)));
				}
			}
		}
	} else if (upper.find("ROW") == 0) {
		result.type_root = PaimonTypeRoot::STRUCT;
		// Parse ROW<f1 TYPE1, f2 TYPE2, ...>
		auto angle_start = upper.find('<');
		if (angle_start != string::npos) {
			auto angle_end = upper.rfind('>');
			if (angle_end != string::npos) {
				string inner = type_str.substr(angle_start + 1, angle_end - angle_start - 1);
				// Split by commas at depth 0
				int depth = 0;
				vector<string> field_strs;
				idx_t start = 0;
				for (idx_t i = 0; i < inner.size(); i++) {
					if (inner[i] == '<') {
						depth++;
					} else if (inner[i] == '>') {
						depth--;
					} else if (inner[i] == ',' && depth == 0) {
						string field_part = inner.substr(start, i - start);
						StringUtil::Trim(field_part);
						field_strs.push_back(field_part);
						start = i + 1;
					}
				}
				string last_field = inner.substr(start);
				StringUtil::Trim(last_field);
				field_strs.push_back(last_field);

				int field_id = 0;
				for (auto &field_str : field_strs) {
					PaimonSchemaField field;
					field.id = field_id++;
					// Each field is "name TYPE [NOT NULL]"
					auto space_pos = field_str.find(' ');
					if (space_pos != string::npos) {
						string name_part = field_str.substr(0, space_pos);
						StringUtil::Trim(name_part);
						field.name = name_part;
						string remaining = field_str.substr(space_pos + 1);
						StringUtil::Trim(remaining);
						// Check for NOT NULL suffix
						if (StringUtil::EndsWith(StringUtil::Upper(remaining), "NOT NULL")) {
							field.nullable = false;
							remaining = remaining.substr(0, remaining.size() - 8);
							StringUtil::Trim(remaining);
						}
						field.type = ParsePaimonTypeString(remaining);
					} else {
						field.name = field_str;
						field.type.type_root = PaimonTypeRoot::STRING;
					}
					result.fields.push_back(std::move(field));
				}
			}
		}
	} else {
		// Unknown type, default to string
		result.type_root = PaimonTypeRoot::STRING;
	}

	return result;
}

//===--------------------------------------------------------------------===//
// Schema File Loading
//===--------------------------------------------------------------------===//

static unique_ptr<PaimonSchema> LoadSchemaFile(const string &table_location, int64_t schema_id, FileSystem &fs) {
	// Paimon schema files are at: {table}/schema/schema-{id}
	string schema_path = table_location + "/schema/schema-" + std::to_string(schema_id);

	if (!fs.FileExists(schema_path)) {
		throw IOException("Paimon schema file not found: " + schema_path);
	}

	string json_content = IcebergUtils::FileToString(schema_path, fs);

	auto doc = std::unique_ptr<yyjson_doc, YyjsonDocDeleter>(
	    yyjson_read(json_content.c_str(), json_content.size(), 0));

	if (!doc) {
		throw InvalidInputException("Failed to parse Paimon schema JSON from: " + schema_path);
	}

	auto root = yyjson_doc_get_root(doc.get());
	if (!root || !yyjson_is_obj(root)) {
		throw InvalidInputException("Invalid Paimon schema JSON: expected object root");
	}

	auto schema = make_uniq<PaimonSchema>();

	// Parse schema ID
	auto id_val = yyjson_obj_get(root, "id");
	if (id_val && yyjson_is_int(id_val)) {
		schema->id = yyjson_get_int(id_val);
	} else {
		schema->id = schema_id;
	}

	// Parse fields array
	auto fields_val = yyjson_obj_get(root, "fields");
	if (fields_val && yyjson_is_arr(fields_val)) {
		size_t idx, max;
		yyjson_val *field_val;
		yyjson_arr_foreach(fields_val, idx, max, field_val) {
			PaimonSchemaField field;

			auto fid = yyjson_obj_get(field_val, "id");
			if (fid && yyjson_is_int(fid)) {
				field.id = yyjson_get_int(fid);
			}

			auto fname = yyjson_obj_get(field_val, "name");
			if (fname && yyjson_is_str(fname)) {
				field.name = yyjson_get_str(fname);
			}

			auto ftype = yyjson_obj_get(field_val, "type");
			if (ftype && yyjson_is_str(ftype)) {
				// Simple type string like "int", "string", "DECIMAL(10,2)"
				field.type = ParsePaimonTypeString(yyjson_get_str(ftype));
			} else if (ftype && yyjson_is_obj(ftype)) {
				// Complex type object — parse recursively
				auto type_name = yyjson_obj_get(ftype, "type");
				if (type_name && yyjson_is_str(type_name)) {
					field.type = ParsePaimonTypeString(yyjson_get_str(type_name));
				}
				// Could also have "element" for ARRAY, "key"/"value" for MAP, "fields" for ROW
				auto element = yyjson_obj_get(ftype, "element");
				if (element && field.type.type_root == PaimonTypeRoot::ARRAY) {
					if (yyjson_is_str(element)) {
						field.type.element_type = make_uniq<PaimonDataType>(ParsePaimonTypeString(yyjson_get_str(element)));
					}
				}
				auto key = yyjson_obj_get(ftype, "key");
				auto value = yyjson_obj_get(ftype, "value");
				if (key && value && field.type.type_root == PaimonTypeRoot::MAP) {
					if (yyjson_is_str(key)) {
						field.type.key_type = make_uniq<PaimonDataType>(ParsePaimonTypeString(yyjson_get_str(key)));
					}
					if (yyjson_is_str(value)) {
						field.type.value_type = make_uniq<PaimonDataType>(ParsePaimonTypeString(yyjson_get_str(value)));
					}
				}
			}

			auto fnullable = yyjson_obj_get(field_val, "nullable");
			if (fnullable) {
				if (yyjson_is_bool(fnullable)) {
					field.nullable = yyjson_get_bool(fnullable);
				} else if (yyjson_is_str(fnullable)) {
					string nstr = yyjson_get_str(fnullable);
					field.nullable = (nstr != "false" && nstr != "NOT NULL");
				}
			}

			schema->fields.push_back(std::move(field));
		}
	}

	// Parse partition keys
	auto partition_keys_val = yyjson_obj_get(root, "partitionKeys");
	if (partition_keys_val && yyjson_is_arr(partition_keys_val)) {
		size_t idx, max;
		yyjson_val *key_val;
		yyjson_arr_foreach(partition_keys_val, idx, max, key_val) {
			if (yyjson_is_str(key_val)) {
				schema->partition_keys.push_back(yyjson_get_str(key_val));
			}
		}
	}

	// Parse primary keys
	auto primary_keys_val = yyjson_obj_get(root, "primaryKeys");
	if (primary_keys_val && yyjson_is_arr(primary_keys_val)) {
		size_t idx, max;
		yyjson_val *key_val;
		yyjson_arr_foreach(primary_keys_val, idx, max, key_val) {
			if (yyjson_is_str(key_val)) {
				schema->primary_keys.push_back(yyjson_get_str(key_val));
			}
		}
	}

	// Parse table options
	auto options_val = yyjson_obj_get(root, "options");
	if (options_val && yyjson_is_obj(options_val)) {
		yyjson_obj_iter iter;
		yyjson_obj_iter_init(options_val, &iter);
		yyjson_val *key;
		while ((key = yyjson_obj_iter_next(&iter))) {
			auto val = yyjson_obj_iter_get_val(key);
			if (yyjson_is_str(val)) {
				schema->options[yyjson_get_str(key)] = yyjson_get_str(val);
			}
		}
	}

	return schema;
}

//===--------------------------------------------------------------------===//
// Snapshot Parsing
//===--------------------------------------------------------------------===//

static PaimonSnapshot ParseSnapshotJson(const string &json_content, const string &path) {
	auto doc = std::unique_ptr<yyjson_doc, YyjsonDocDeleter>(
	    yyjson_read(json_content.c_str(), json_content.size(), 0));

	if (!doc) {
		throw InvalidInputException("Failed to parse Paimon snapshot JSON from: " + path);
	}

	auto root = yyjson_doc_get_root(doc.get());
	if (!root || !yyjson_is_obj(root)) {
		throw InvalidInputException("Invalid Paimon snapshot JSON: expected object root in " + path);
	}

	PaimonSnapshot snapshot;

	// version
	auto version_val = yyjson_obj_get(root, "version");
	if (version_val && yyjson_is_int(version_val)) {
		snapshot.version = yyjson_get_int(version_val);
	}

	// id (snapshot ID)
	auto id_val = yyjson_obj_get(root, "id");
	if (id_val && yyjson_is_int(id_val)) {
		snapshot.snapshot_id = yyjson_get_uint(id_val);
	}

	// schemaId
	auto schema_id_val = yyjson_obj_get(root, "schemaId");
	if (schema_id_val && yyjson_is_int(schema_id_val)) {
		snapshot.schema_id = yyjson_get_int(schema_id_val);
	}

	// baseManifestList
	auto base_manifest_val = yyjson_obj_get(root, "baseManifestList");
	if (base_manifest_val && yyjson_is_str(base_manifest_val)) {
		snapshot.base_manifest_list = yyjson_get_str(base_manifest_val);
	}

	// deltaManifestList
	auto delta_manifest_val = yyjson_obj_get(root, "deltaManifestList");
	if (delta_manifest_val && yyjson_is_str(delta_manifest_val)) {
		snapshot.delta_manifest_list = yyjson_get_str(delta_manifest_val);
	}

	// changelogManifestList (nullable)
	auto changelog_val = yyjson_obj_get(root, "changelogManifestList");
	if (changelog_val && yyjson_is_str(changelog_val)) {
		snapshot.changelog_manifest_list_str = yyjson_get_str(changelog_val);
	}

	// indexManifest (nullable)
	auto index_val = yyjson_obj_get(root, "indexManifest");
	if (index_val && yyjson_is_str(index_val)) {
		snapshot.index_manifest_str = yyjson_get_str(index_val);
	}

	// commitUser
	auto commit_user_val = yyjson_obj_get(root, "commitUser");
	if (commit_user_val && yyjson_is_str(commit_user_val)) {
		snapshot.commit_user = yyjson_get_str(commit_user_val);
	}

	// commitIdentifier
	auto commit_id_val = yyjson_obj_get(root, "commitIdentifier");
	if (commit_id_val && yyjson_is_int(commit_id_val)) {
		snapshot.commit_identifier = yyjson_get_sint(commit_id_val);
	}

	// commitKind
	auto commit_kind_val = yyjson_obj_get(root, "commitKind");
	if (commit_kind_val && yyjson_is_str(commit_kind_val)) {
		snapshot.commit_kind = yyjson_get_str(commit_kind_val);
	}

	// timeMillis / timestampMs (Paimon uses both names depending on version)
	auto time_val = yyjson_obj_get(root, "timeMillis");
	if (!time_val) {
		time_val = yyjson_obj_get(root, "timestampMs");
	}
	if (time_val && yyjson_is_int(time_val)) {
		snapshot.timestamp_ms = yyjson_get_sint(time_val);
	}

	// logOffsets
	auto log_offsets_val = yyjson_obj_get(root, "logOffsets");
	if (log_offsets_val && yyjson_is_str(log_offsets_val)) {
		snapshot.log_offsets = yyjson_get_str(log_offsets_val);
	}

	// totalRecordCount
	auto total_val = yyjson_obj_get(root, "totalRecordCount");
	if (total_val && yyjson_is_int(total_val)) {
		snapshot.total_record_count_val = yyjson_get_sint(total_val);
		snapshot.has_total_record_count = true;
	}

	// deltaRecordCount
	auto delta_rec_val = yyjson_obj_get(root, "deltaRecordCount");
	if (delta_rec_val && yyjson_is_int(delta_rec_val)) {
		snapshot.delta_record_count_val = yyjson_get_sint(delta_rec_val);
		snapshot.has_delta_record_count = true;
	}

	// changelogRecordCount
	auto changelog_rec_val = yyjson_obj_get(root, "changelogRecordCount");
	if (changelog_rec_val && yyjson_is_int(changelog_rec_val)) {
		snapshot.changelog_record_count_val = yyjson_get_sint(changelog_rec_val);
		snapshot.has_changelog_record_count = true;
	}

	// watermark
	auto watermark_val = yyjson_obj_get(root, "watermark");
	if (watermark_val && yyjson_is_int(watermark_val)) {
		snapshot.watermark_val = yyjson_get_sint(watermark_val);
		snapshot.has_watermark = true;
	}

	// Legacy: set manifest_list for backward compat
	if (!snapshot.delta_manifest_list.empty()) {
		snapshot.manifest_list = snapshot.delta_manifest_list;
	} else if (!snapshot.base_manifest_list.empty()) {
		snapshot.manifest_list = snapshot.base_manifest_list;
	}

	// Legacy: sequence_number = snapshot_id (Paimon doesn't have a separate sequence)
	snapshot.sequence_number = snapshot.snapshot_id;

	return snapshot;
}

//===--------------------------------------------------------------------===//
// PaimonTableMetadata Implementation
//===--------------------------------------------------------------------===//

string PaimonTableMetadata::GetMetaDataPath(ClientContext &context, const string &table_location, FileSystem &fs,
                                            const PaimonOptions &options) {
	string snapshot_dir = table_location + "/snapshot";

	if (!fs.DirectoryExists(snapshot_dir)) {
		throw IOException("Paimon snapshot directory does not exist: " + snapshot_dir);
	}

	string snapshot_filename;

	switch (options.snapshot_lookup.snapshot_source) {
	case PaimonOptions::SnapshotLookup::SnapshotSource::FROM_ID: {
		snapshot_filename = "snapshot-" + std::to_string(options.snapshot_lookup.snapshot_id);
		break;
	}
	case PaimonOptions::SnapshotLookup::SnapshotSource::FROM_TIMESTAMP: {
		// Scan all snapshot files to find the right one
		vector<string> snapshot_files;
		fs.ListFiles(snapshot_dir, [&](const string &fname, bool is_dir) {
			if (!is_dir && fname.find("snapshot-") == 0) {
				snapshot_files.push_back(fname);
			}
		});

		if (snapshot_files.empty()) {
			throw IOException("No snapshot files found in: " + snapshot_dir);
		}

		int64_t requested_time = options.snapshot_lookup.snapshot_timestamp.value;
		int64_t best_timestamp = NumericLimits<int64_t>::Minimum();
		string best_snapshot_file;

		for (const auto &snapshot_file : snapshot_files) {
			try {
				string full_path = snapshot_dir + "/" + snapshot_file;
				string json_content = IcebergUtils::FileToString(full_path, fs);
				auto snapshot = ParseSnapshotJson(json_content, full_path);

				if (snapshot.timestamp_ms <= requested_time && snapshot.timestamp_ms > best_timestamp) {
					best_timestamp = snapshot.timestamp_ms;
					best_snapshot_file = snapshot_file;
				}
			} catch (const std::exception &e) {
				// Skip malformed snapshot files
			}
		}

		if (best_snapshot_file.empty()) {
			throw IOException("No snapshot found for requested timestamp in: " + snapshot_dir);
		}

		snapshot_filename = best_snapshot_file;
		break;
	}
	case PaimonOptions::SnapshotLookup::SnapshotSource::LATEST:
	default: {
		if (options.table_version == "latest") {
			// Look for LATEST file first
			string latest_file = snapshot_dir + "/LATEST";
			if (fs.FileExists(latest_file)) {
				snapshot_filename = IcebergUtils::FileToString(latest_file, fs);
				StringUtil::Trim(snapshot_filename);
			} else {
				// Find the highest numbered snapshot file
				vector<string> files;
				fs.ListFiles(snapshot_dir, [&](const string &fname, bool is_dir) {
					if (!is_dir && fname.find("snapshot-") == 0) {
						files.push_back(fname);
					}
				});

				if (files.empty()) {
					throw IOException("No snapshot files found in: " + snapshot_dir);
				}

				sort(files.begin(), files.end());
				snapshot_filename = files.back();
			}
		} else {
			snapshot_filename = "snapshot-" + options.table_version;
		}
		break;
	}
	}

	string full_path = snapshot_dir + "/" + snapshot_filename;
	if (!fs.FileExists(full_path)) {
		throw IOException("Snapshot file not found: " + full_path);
	}

	return full_path;
}

SnapshotMetadata PaimonTableMetadata::ParseSnapshotMetadata(const string &metadata_path, FileSystem &fs,
                                                            const string &compression_codec) {
	SnapshotMetadata result;

	string json_content = IcebergUtils::FileToString(metadata_path, fs);
	auto snapshot = ParseSnapshotJson(json_content, metadata_path);

	result.timestamp_ms = snapshot.timestamp_ms;
	result.snapshot_id = snapshot.snapshot_id;

	return result;
}

unique_ptr<PaimonTableMetadata> PaimonTableMetadata::Parse(const string &metadata_path, FileSystem &fs,
                                                           const string &compression_codec) {
	auto result = make_uniq<PaimonTableMetadata>();

	if (!fs.FileExists(metadata_path)) {
		throw IOException("Paimon metadata file does not exist: " + metadata_path);
	}

	// Read and parse the snapshot file
	string json_content = IcebergUtils::FileToString(metadata_path, fs);
	auto snapshot = ParseSnapshotJson(json_content, metadata_path);

	// Store the snapshot
	result->snapshots[snapshot.snapshot_id] = snapshot;

	// Derive the table location from the metadata path
	// metadata_path is like: /path/to/table/snapshot/snapshot-1
	// table_location is: /path/to/table
	string table_location;
	auto snapshot_dir_pos = metadata_path.rfind("/snapshot/");
	if (snapshot_dir_pos != string::npos) {
		table_location = metadata_path.substr(0, snapshot_dir_pos);
	} else {
		throw IOException("Cannot determine table location from metadata path: " + metadata_path);
	}

	result->table_location = table_location;

	// Load the schema file referenced by the snapshot
	try {
		result->schema = LoadSchemaFile(table_location, snapshot.schema_id, fs);
	} catch (const std::exception &e) {
		// If schema loading fails, try schema-0 as fallback (some tables use 0-based IDs)
		try {
			if (snapshot.schema_id != 0) {
				result->schema = LoadSchemaFile(table_location, 0, fs);
			} else {
				throw;
			}
		} catch (...) {
			throw IOException("Failed to load Paimon schema file for schemaId=" +
			                  std::to_string(snapshot.schema_id) + ": " + string(e.what()));
		}
	}

	result->table_format_version = std::to_string(snapshot.version);

	return result;
}

PaimonSnapshot *PaimonTableMetadata::FindSnapshotByTimestamp(timestamp_t timestamp) {
	PaimonSnapshot *best_snapshot = nullptr;
	int64_t best_timestamp = NumericLimits<int64_t>::Minimum();

	for (auto &pair : snapshots) {
		PaimonSnapshot &snapshot = pair.second;
		if (snapshot.timestamp_ms <= timestamp.value && snapshot.timestamp_ms > best_timestamp) {
			best_snapshot = &snapshot;
			best_timestamp = snapshot.timestamp_ms;
		}
	}

	return best_snapshot;
}

PaimonSnapshot *PaimonTableMetadata::FindSnapshotById(uint64_t snapshot_id) {
	auto it = snapshots.find(snapshot_id);
	if (it != snapshots.end()) {
		return &it->second;
	}
	return nullptr;
}

PaimonSnapshot *PaimonTableMetadata::GetCurrentSnapshot(const PaimonOptions &options) {
	switch (options.snapshot_lookup.snapshot_source) {
	case PaimonOptions::SnapshotLookup::SnapshotSource::LATEST: {
		PaimonSnapshot *latest = nullptr;
		for (auto &pair : snapshots) {
			if (!latest || pair.second.snapshot_id > latest->snapshot_id) {
				latest = &pair.second;
			}
		}
		return latest;
	}
	case PaimonOptions::SnapshotLookup::SnapshotSource::FROM_ID: {
		return FindSnapshotById(options.snapshot_lookup.snapshot_id);
	}
	case PaimonOptions::SnapshotLookup::SnapshotSource::FROM_TIMESTAMP: {
		return FindSnapshotByTimestamp(options.snapshot_lookup.snapshot_timestamp);
	}
	default:
		return nullptr;
	}
}

// These are kept for API compatibility but the real parsing is above
void PaimonTableMetadata::ParseSchemaFromJson(yyjson_val *schema_obj, PaimonSchema &schema) {
	auto fields_obj = yyjson_obj_get(schema_obj, "fields");
	if (fields_obj && yyjson_is_arr(fields_obj)) {
		size_t idx, max;
		yyjson_val *field_val;
		yyjson_arr_foreach(fields_obj, idx, max, field_val) {
			PaimonSchemaField field;
			ParseSchemaFieldFromJson(field_val, field);
			schema.fields.emplace_back(std::move(field));
		}
	}
}

void PaimonTableMetadata::ParseSchemaFieldFromJson(yyjson_val *field_obj, PaimonSchemaField &field) {
	auto id_obj = yyjson_obj_get(field_obj, "id");
	if (id_obj && yyjson_is_int(id_obj)) {
		field.id = yyjson_get_int(id_obj);
	}

	auto name_obj = yyjson_obj_get(field_obj, "name");
	if (name_obj && yyjson_is_str(name_obj)) {
		field.name = yyjson_get_str(name_obj);
	}

	auto type_obj = yyjson_obj_get(field_obj, "type");
	if (type_obj && yyjson_is_str(type_obj)) {
		field.type = ParsePaimonTypeString(yyjson_get_str(type_obj));
	}
}

void PaimonTableMetadata::ParseDataTypeFromJson(yyjson_val *type_obj, PaimonDataType &data_type) {
	if (yyjson_is_str(type_obj)) {
		data_type = ParsePaimonTypeString(yyjson_get_str(type_obj));
	}
}

PaimonTypeRoot PaimonTableMetadata::StringToTypeRoot(const string &type_str) {
	auto dt = ParsePaimonTypeString(type_str);
	return dt.type_root;
}

void PaimonTableMetadata::CreateDefaultSchema(PaimonSchema &schema) {
	// This should not be called in normal operation.
	// It exists only as a last resort if schema files are missing.
	PaimonSchemaField f1;
	f1.id = 0;
	f1.name = "data";
	f1.type.type_root = PaimonTypeRoot::STRING;
	schema.fields.push_back(std::move(f1));
}

//===--------------------------------------------------------------------===//
// FileStorePathFactory Implementation
//===--------------------------------------------------------------------===//

FileStorePathFactory::FileStorePathFactory(const std::string &tablePath, int numBuckets)
    : tablePath(tablePath), numBuckets(numBuckets) {
}

std::string FileStorePathFactory::bucketPath(int bucket) const {
	return tablePath + "/bucket-" + std::to_string(bucket);
}

std::string FileStorePathFactory::dataFilePath(int bucket, const std::string &uuid, int counter,
                                               PaimonFileFormat format) const {
	return bucketPath(bucket) + "/data-" + uuid + "-" + std::to_string(counter) + getFormatExtension(format);
}

std::string FileStorePathFactory::deleteFilePath(int bucket, const std::string &uuid, int counter,
                                                 PaimonFileFormat format) const {
	return bucketPath(bucket) + "/delete-" + uuid + "-" + std::to_string(counter) + getFormatExtension(format);
}

std::string FileStorePathFactory::manifestFilePath(const std::string &uuid, int index) const {
	return tablePath + "/manifest/manifest-" + uuid + "-" + std::to_string(index) + ".avro";
}

std::string FileStorePathFactory::manifestListFilePath(const std::string &uuid, int index) const {
	return tablePath + "/manifest/manifest-list-" + uuid + "-" + std::to_string(index) + ".avro";
}

std::string FileStorePathFactory::snapshotFilePath(int64_t snapshotId) const {
	return tablePath + "/snapshot/snapshot-" + std::to_string(snapshotId);
}

std::string FileStorePathFactory::earliestPointerPath() const {
	return tablePath + "/snapshot/EARLIEST";
}

std::string FileStorePathFactory::latestPointerPath() const {
	return tablePath + "/snapshot/LATEST";
}

std::string FileStorePathFactory::partitionBucketPath(
    const std::vector<std::pair<std::string, std::string>> &partition, int bucket) const {
	std::string path = tablePath;
	for (const auto &part : partition) {
		path += "/" + part.first + "=" + part.second;
	}
	path += "/bucket-" + std::to_string(bucket);
	return path;
}

std::string FileStorePathFactory::partitionedDataFilePath(
    const std::vector<std::pair<std::string, std::string>> &partition, int bucket, const std::string &uuid,
    int counter, PaimonFileFormat format) const {
	return partitionBucketPath(partition, bucket) + "/data-" + uuid + "-" + std::to_string(counter) +
	       getFormatExtension(format);
}

std::string FileStorePathFactory::partitionedDeleteFilePath(
    const std::vector<std::pair<std::string, std::string>> &partition, int bucket, const std::string &uuid,
    int counter, PaimonFileFormat format) const {
	return partitionBucketPath(partition, bucket) + "/delete-" + uuid + "-" + std::to_string(counter) +
	       getFormatExtension(format);
}

std::string FileStorePathFactory::getFormatExtension(PaimonFileFormat format) const {
	switch (format) {
	case PaimonFileFormat::PARQUET:
		return ".parquet";
	case PaimonFileFormat::ORC:
		return ".orc";
	case PaimonFileFormat::AVRO:
		return ".avro";
	default:
		return ".orc";
	}
}

//===--------------------------------------------------------------------===//
// BucketManager Implementation
//===--------------------------------------------------------------------===//

BucketManager::BucketManager(int numBuckets) : numBuckets(numBuckets) {
}

int BucketManager::assignBucket(const std::string &key) const {
	size_t hashValue = hasher(key);
	return static_cast<int>(hashValue % numBuckets);
}

int BucketManager::assignBucket(const std::vector<Value> &partitionValues, const Value &primaryKey) const {
	std::string compositeKey;
	for (const auto &partValue : partitionValues) {
		compositeKey += partValue.ToString() + "|";
	}
	compositeKey += primaryKey.ToString();
	return assignBucket(compositeKey);
}

std::vector<int> BucketManager::getAllBuckets() const {
	std::vector<int> buckets;
	for (int i = 0; i < numBuckets; i++) {
		buckets.push_back(i);
	}
	return buckets;
}

} // namespace duckdb
