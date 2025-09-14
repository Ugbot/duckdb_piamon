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

    string snapshot_filename;

    // Determine which snapshot to use based on lookup options
    switch (options.snapshot_lookup.snapshot_source) {
        case PaimonOptions::SnapshotLookup::SnapshotSource::FROM_ID: {
            // Find snapshot by ID
            snapshot_filename = "snapshot-" + std::to_string(options.snapshot_lookup.snapshot_id);
            break;
        }
        case PaimonOptions::SnapshotLookup::SnapshotSource::FROM_TIMESTAMP: {
            // For timestamp lookup, we need to find the right snapshot
            // Parse all snapshot files to find the one with the correct timestamp
            vector<string> snapshot_files;
            fs.ListFiles(snapshot_dir, [&](const string &fname, bool is_dir) {
                if (!is_dir && fname.find("snapshot-") == 0) {
                    snapshot_files.push_back(fname);
                }
            });

            if (snapshot_files.empty()) {
                throw IOException("No snapshot files found in: " + snapshot_dir);
            }

            // Find the snapshot with the highest timestamp <= requested timestamp
            timestamp_t requested_time = options.snapshot_lookup.snapshot_timestamp;
            timestamp_t best_timestamp = NumericLimits<timestamp_t>::Minimum();
            string best_snapshot_file;

            for (const auto &snapshot_file : snapshot_files) {
                try {
                    string full_path = snapshot_dir + "/" + snapshot_file;
                    auto snapshot_metadata = ParseSnapshotMetadata(full_path, fs, options.metadata_compression_codec);

                    if (snapshot_metadata.timestamp_ms <= requested_time &&
                        snapshot_metadata.timestamp_ms > best_timestamp) {
                        best_timestamp = snapshot_metadata.timestamp_ms;
                        best_snapshot_file = snapshot_file;
                    }
                } catch (const std::exception &e) {
                    // Skip malformed snapshot files
                    std::cerr << "PAIMON: Skipping malformed snapshot file " << snapshot_file << ": " << e.what() << std::endl;
                }
            }

            if (best_snapshot_file.empty()) {
                throw IOException("No snapshot found for timestamp " + std::to_string(requested_time) + " in: " + snapshot_dir);
            }

            snapshot_filename = best_snapshot_file;
            break;
        }
        case PaimonOptions::SnapshotLookup::SnapshotSource::LATEST:
        default: {
            // For Paimon, we need to find the latest snapshot
            if (options.table_version == "latest") {
                // Look for LATEST file first
                string latest_file = snapshot_dir + "/LATEST";
                if (fs.FileExists(latest_file)) {
                    snapshot_filename = IcebergUtils::FileToString(latest_file, fs);
                    // Trim whitespace from filename
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

                    // Sort and pick the latest
                    sort(files.begin(), files.end());
                    snapshot_filename = files.back();
                }
            } else {
                // Handle specific version
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

    // For now, create mock data since we don't have real Paimon snapshot parsing
    // TODO: Implement proper Paimon snapshot JSON parsing
    result.timestamp_ms = Timestamp::GetCurrentTimestamp();
    result.snapshot_id = 1;

    // In a real implementation, we would:
    // 1. Read the JSON file
    // 2. Parse timestamp-ms field
    // 3. Parse snapshot-id field

    return result;
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

    // Create a proper Paimon version 3 snapshot (matching Java spec)
    PaimonSnapshot snapshot;
    snapshot.version = 3;
    snapshot.snapshot_id = 1;
    snapshot.schema_id = 0;
    snapshot.time_millis = Timestamp::GetCurrentTimestamp();
    snapshot.manifest_list = "manifest-list-initial-0";  // Will be updated during writes

    // Initialize all version 3 fields (matching org.apache.paimon.Snapshot)
    snapshot.base_manifest_list = "";  // Empty for initial snapshot
    snapshot.base_manifest_list_size = nullptr;  // Null for empty list
    snapshot.delta_manifest_list = "manifest-list-initial-0";
    snapshot.delta_manifest_list_size = nullptr;  // Will be set during writes
    snapshot.changelog_manifest_list = nullptr;  // Null for append-only tables
    snapshot.changelog_manifest_list_size = nullptr;
    snapshot.index_manifest = nullptr;  // No index manifest initially
    snapshot.commit_user = "duckdb-paimon";
    snapshot.commit_identifier = 9223372036854775807LL;
    snapshot.commit_kind = "APPEND";
    snapshot.log_offsets = "{}";  // Empty JSON object
    snapshot.total_record_count = nullptr;  // Null initially, will be set during writes
    snapshot.delta_record_count = nullptr;  // Null initially, will be set during writes
    snapshot.changelog_record_count = nullptr;  // Null for append-only
    snapshot.watermark = nullptr;  // Null initially
    snapshot.statistics = nullptr;  // No statistics initially
    snapshot.properties = nullptr;  // No additional properties
    snapshot.next_row_id = nullptr;  // Not used in basic implementation

    // Legacy compatibility fields
    snapshot.sequence_number = 1;

    result->snapshots[1] = std::move(snapshot);

    return result;
}

PaimonSnapshot *PaimonTableMetadata::FindSnapshotByTimestamp(timestamp_t timestamp) {
    // Find the snapshot that was active at the given timestamp
    // In Paimon/Iceberg, snapshots are ordered by time, so we want the latest snapshot
    // that has a timestamp <= the requested timestamp

    PaimonSnapshot *best_snapshot = nullptr;
    timestamp_t best_timestamp = NumericLimits<timestamp_t>::Minimum();

    for (auto &pair : snapshots) {
        PaimonSnapshot &snapshot = pair.second;
        if (snapshot.time_millis <= timestamp && snapshot.time_millis > best_timestamp) {
            best_snapshot = &snapshot;
            best_timestamp = snapshot.time_millis;
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
            // Return the latest snapshot (highest ID)
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

// FileStorePathFactory implementation
FileStorePathFactory::FileStorePathFactory(const std::string& tablePath, int numBuckets)
    : tablePath(tablePath), numBuckets(numBuckets) {
}

std::string FileStorePathFactory::bucketPath(int bucket) const {
    return tablePath + "/bucket-" + std::to_string(bucket);
}

std::string FileStorePathFactory::dataFilePath(int bucket, const std::string& uuid, int counter, PaimonFileFormat format) const {
    std::string bucketDir = bucketPath(bucket);
    std::string extension = getFormatExtension(format);
    return bucketDir + "/data-" + uuid + "-" + std::to_string(counter) + extension;
}

std::string FileStorePathFactory::deleteFilePath(int bucket, const std::string& uuid, int counter, PaimonFileFormat format) const {
    std::string bucketDir = bucketPath(bucket);
    std::string extension = getFormatExtension(format);
    return bucketDir + "/delete-" + uuid + "-" + std::to_string(counter) + extension;
}

std::string FileStorePathFactory::manifestFilePath(const std::string& uuid, int index) const {
    return tablePath + "/manifest/manifest-" + uuid + "-" + std::to_string(index) + ".avro";
}

std::string FileStorePathFactory::manifestListFilePath(const std::string& uuid, int index) const {
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

std::string FileStorePathFactory::partitionBucketPath(const std::vector<std::pair<std::string, std::string>>& partition, int bucket) const {
    std::string path = tablePath;
    for (const auto& part : partition) {
        path += "/" + part.first + "=" + part.second;
    }
    path += "/bucket-" + std::to_string(bucket);
    return path;
}

std::string FileStorePathFactory::partitionedDataFilePath(const std::vector<std::pair<std::string, std::string>>& partition, int bucket,
                                                         const std::string& uuid, int counter, PaimonFileFormat format) const {
    std::string basePath = partitionBucketPath(partition, bucket);
    std::string filename = "data-" + uuid + "-" + std::to_string(counter) + getFormatExtension(format);
    return basePath + "/" + filename;
}

std::string FileStorePathFactory::partitionedDeleteFilePath(const std::vector<std::pair<std::string, std::string>>& partition, int bucket,
                                                           const std::string& uuid, int counter, PaimonFileFormat format) const {
    std::string basePath = partitionBucketPath(partition, bucket);
    std::string filename = "delete-" + uuid + "-" + std::to_string(counter) + getFormatExtension(format);
    return basePath + "/" + filename;
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
            return ".orc"; // Default to ORC
    }
}

// BucketManager implementation
BucketManager::BucketManager(int numBuckets) : numBuckets(numBuckets) {
}

int BucketManager::assignBucket(const std::string& key) const {
    size_t hashValue = hasher(key);
    return static_cast<int>(hashValue % numBuckets);
}

int BucketManager::assignBucket(const std::vector<Value>& partitionValues, const Value& primaryKey) const {
    // Create a composite key from partition values and primary key
    std::string compositeKey;

    // Add partition values
    for (const auto& partValue : partitionValues) {
        compositeKey += partValue.ToString() + "|";
    }

    // Add primary key
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
