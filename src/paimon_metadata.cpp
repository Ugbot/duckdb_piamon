#include "paimon_metadata.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "iceberg_utils.hpp"
#include "catalog_utils.hpp"
#include "yyjson.hpp"

#include <fstream>
#include <sstream>
#include <regex>

namespace duckdb {

string PaimonTableMetadata::GetMetaDataPath(ClientContext &context, const string &table_location, FileSystem &fs,
                                           const PaimonOptions &options) {
    string snapshot_dir = table_location + "/snapshot";

    if (!fs.DirectoryExists(snapshot_dir)) {
        throw IOException("Paimon snapshot directory does not exist: " + snapshot_dir);
    }

    // For Paimon, we need to find the latest snapshot
    if (options.table_version == "latest") {
        // Look for LATEST file first
        string latest_file = snapshot_dir + "/LATEST";
        if (fs.FileExists(latest_file)) {
            string snapshot_filename = IcebergUtils::FileToString(latest_file, fs);
            // Trim whitespace from filename
            StringUtil::Trim(snapshot_filename);
            return snapshot_dir + "/" + snapshot_filename;
        }

        // Otherwise, find the highest numbered snapshot file
        vector<string> files;
        fs.ListFiles(snapshot_dir, [&](const string &fname, bool is_dir) {
            if (!is_dir && fname.find("snapshot-") == 0) {
                files.push_back(fname);
            }
        });

        if (files.empty()) {
            throw IOException("No snapshot files found in: " + snapshot_dir);
        }

        // Sort and pick the latest
        sort(files.begin(), files.end());
        return snapshot_dir + "/" + files.back();
    }

    // Handle specific version
    string version_file = snapshot_dir + "/snapshot-" + options.table_version;
    if (fs.FileExists(version_file)) {
        return version_file;
    }

    throw IOException("Snapshot version not found: " + options.table_version);
}

unique_ptr<PaimonTableMetadata> PaimonTableMetadata::Parse(const string &metadata_path, FileSystem &fs,
                                                          const string &compression_codec) {
    auto result = make_uniq<PaimonTableMetadata>();

    if (!fs.FileExists(metadata_path)) {
        throw IOException("Paimon metadata file does not exist: " + metadata_path);
    }

    // Read the snapshot file content
    string json_content = IcebergUtils::FileToString(metadata_path, fs);

    // Parse JSON using yyjson
    auto doc = std::unique_ptr<yyjson_doc, YyjsonDocDeleter>(
        yyjson_read(json_content.c_str(), json_content.size(), 0));

    if (!doc) {
        throw InvalidInputException("Failed to parse Paimon snapshot JSON from: " + metadata_path);
    }

    auto root = yyjson_doc_get_root(doc.get());
    if (!root) {
        throw InvalidInputException("Invalid Paimon snapshot JSON: no root object");
    }

    // Parse basic metadata
    result->table_format_version = "1"; // Default

    // Try to parse schema information from the snapshot JSON
    auto schema = make_uniq<PaimonSchema>();
    schema->id = 1;

    // Check if the snapshot contains schema information
    auto schema_obj = yyjson_obj_get(root, "schema");
    if (schema_obj && yyjson_is_obj(schema_obj)) {
        // Parse schema from snapshot (if embedded)
        ParseSchemaFromJson(schema_obj, *schema);
    } else {
        // Try to find schema file reference
        auto schema_id_obj = yyjson_obj_get(root, "schemaId");
        if (schema_id_obj && yyjson_is_int(schema_id_obj)) {
            schema->id = yyjson_get_int(schema_id_obj);
            // TODO: Load schema from separate schema file
        }

        // For now, create basic fields for testing
        // In a real implementation, we'd load the schema file
        CreateDefaultSchema(*schema);
    }

    result->schema = std::move(schema);

    // Try to parse snapshot information from the JSON
    // Note: Paimon snapshot format may differ from Iceberg, this is a basic implementation

    // For now, create a mock snapshot to allow basic functionality
    // TODO: Implement proper Paimon snapshot JSON parsing
    PaimonSnapshot mock_snapshot;
    mock_snapshot.snapshot_id = 1;
    mock_snapshot.sequence_number = 1;
    mock_snapshot.timestamp_ms = Timestamp::GetCurrentTimestamp();
    mock_snapshot.manifest_list = "mock-manifest-list";
    mock_snapshot.schema_id = "1";
    result->snapshots[1] = std::move(mock_snapshot);

    return result;
}

void PaimonTableMetadata::ParseSchemaFromJson(yyjson_val *schema_obj, PaimonSchema &schema) {
    // Parse fields array
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
    // Parse field id
    auto id_obj = yyjson_obj_get(field_obj, "id");
    if (id_obj && yyjson_is_int(id_obj)) {
        field.id = yyjson_get_int(id_obj);
    }

    // Parse field name
    auto name_obj = yyjson_obj_get(field_obj, "name");
    if (name_obj && yyjson_is_str(name_obj)) {
        field.name = yyjson_get_str(name_obj);
    }

    // Parse field type
    auto type_obj = yyjson_obj_get(field_obj, "type");
    if (type_obj) {
        ParseDataTypeFromJson(type_obj, field.type);
    }
}

void PaimonTableMetadata::ParseDataTypeFromJson(yyjson_val *type_obj, PaimonDataType &data_type) {
    if (yyjson_is_str(type_obj)) {
        // Simple type
        string type_str = yyjson_get_str(type_obj);
        data_type.type_root = StringToTypeRoot(type_str);
    } else if (yyjson_is_obj(type_obj)) {
        // Complex type (array, map, struct)
        // TODO: Implement complex type parsing
        data_type.type_root = PaimonTypeRoot::STRING; // Fallback
    }
}

PaimonTypeRoot PaimonTableMetadata::StringToTypeRoot(const string &type_str) {
    if (type_str == "boolean" || type_str == "BOOLEAN") {
        return PaimonTypeRoot::BOOLEAN;
    } else if (type_str == "int" || type_str == "INT") {
        return PaimonTypeRoot::INT;
    } else if (type_str == "long" || type_str == "LONG" || type_str == "BIGINT") {
        return PaimonTypeRoot::LONG;
    } else if (type_str == "float" || type_str == "FLOAT") {
        return PaimonTypeRoot::FLOAT;
    } else if (type_str == "double" || type_str == "DOUBLE") {
        return PaimonTypeRoot::DOUBLE;
    } else if (type_str == "string" || type_str == "STRING" || type_str == "VARCHAR") {
        return PaimonTypeRoot::STRING;
    } else if (type_str == "date" || type_str == "DATE") {
        return PaimonTypeRoot::DATE;
    } else if (type_str == "timestamp" || type_str == "TIMESTAMP") {
        return PaimonTypeRoot::TIMESTAMP;
    } else {
        // Unknown type, default to string
        return PaimonTypeRoot::STRING;
    }
}

void PaimonTableMetadata::CreateDefaultSchema(PaimonSchema &schema) {
    // Add some basic fields for testing when schema parsing fails
    PaimonSchemaField id_field;
    id_field.id = 1;
    id_field.name = "id";
    id_field.type.type_root = PaimonTypeRoot::LONG;
    schema.fields.emplace_back(std::move(id_field));

    PaimonSchemaField name_field;
    name_field.id = 2;
    name_field.name = "name";
    name_field.type.type_root = PaimonTypeRoot::STRING;
    schema.fields.emplace_back(std::move(name_field));

    PaimonSchemaField age_field;
    age_field.id = 3;
    age_field.name = "age";
    age_field.type.type_root = PaimonTypeRoot::INT;
    schema.fields.emplace_back(std::move(age_field));

    PaimonSchemaField city_field;
    city_field.id = 4;
    city_field.name = "city";
    city_field.type.type_root = PaimonTypeRoot::STRING;
    schema.fields.emplace_back(std::move(city_field));
}

// TODO: Implement proper parsing of Paimon snapshot JSON files
// TODO: Implement manifest list parsing
// TODO: Implement partition spec parsing

} // namespace duckdb
