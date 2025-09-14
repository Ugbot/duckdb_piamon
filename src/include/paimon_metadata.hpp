//===----------------------------------------------------------------------===//
//                         DuckDB
//
// paimon_metadata.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include <memory>
#include <vector>
#include <unordered_map>

namespace duckdb {

class ClientContext;
class FileSystem;
class TableFunctionInput;
class DataChunk;

// Forward declarations for Paimon structures
struct PaimonSnapshot;
struct PaimonManifest;
struct PaimonManifestEntry;
struct PaimonTableMetadata;

// Paimon data type root (similar to Parquet/Arrow types)
enum class PaimonTypeRoot {
    STRING,
    BOOLEAN,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    TIMESTAMP,
    DATE,
    BINARY,
    DECIMAL,
    ARRAY,
    MAP,
    STRUCT
};

// Forward declaration for PaimonSchemaField
struct PaimonSchemaField;

// Paimon data type
struct PaimonDataType {
    PaimonTypeRoot type_root;
    int precision = -1;  // For DECIMAL
    int scale = -1;      // For DECIMAL
    unique_ptr<PaimonDataType> element_type; // For ARRAY
    unique_ptr<PaimonDataType> key_type;     // For MAP
    unique_ptr<PaimonDataType> value_type;   // For MAP
    vector<PaimonSchemaField> fields;        // For STRUCT
};

struct PaimonOptions {
    string metadata_compression_codec = "gzip";
    string table_version = "latest";
    string version_name_format = "v%s%s";

    struct SnapshotLookup {
        enum class SnapshotSource { LATEST, FROM_ID, FROM_TIMESTAMP } snapshot_source = SnapshotSource::LATEST;
        uint64_t snapshot_id = 0;
        timestamp_t snapshot_timestamp;
    } snapshot_lookup;
};

// Paimon snapshot representation (Version 3 format - matching Java spec)
struct PaimonSnapshot {
    // Core version 3 fields (matching org.apache.paimon.Snapshot)
    int version = 3;
    uint64_t snapshot_id = 0;
    int64_t schema_id = 0;
    string base_manifest_list;
    optional_ptr<int64_t> base_manifest_list_size;  // Size of base manifest list
    string delta_manifest_list;
    optional_ptr<int64_t> delta_manifest_list_size;  // Size of delta manifest list
    optional_ptr<string> changelog_manifest_list;    // Can be null
    optional_ptr<int64_t> changelog_manifest_list_size;  // Size of changelog manifest list
    optional_ptr<string> index_manifest;             // Index manifest (can be null)
    string commit_user;
    int64_t commit_identifier = 9223372036854775807LL;
    string commit_kind;
    timestamp_t time_millis;
    string log_offsets;
    optional_ptr<int64_t> total_record_count;        // Can be null
    optional_ptr<int64_t> delta_record_count;        // Can be null
    optional_ptr<int64_t> changelog_record_count;    // Can be null
    optional_ptr<int64_t> watermark;                 // Can be null
    optional_ptr<string> statistics;                 // Statistics (can be null)
    optional_ptr<case_insensitive_map_t<string>> properties;  // Additional properties
    optional_ptr<int64_t> next_row_id;               // Next row ID for assignment

    // Legacy fields for compatibility
    uint64_t sequence_number = 0;
    string manifest_list;  // For backward compatibility

    PaimonSnapshot() = default;
    PaimonSnapshot(const PaimonSnapshot &) = default;
    PaimonSnapshot &operator=(const PaimonSnapshot &) = default;
    PaimonSnapshot(PaimonSnapshot &&) = default;
    PaimonSnapshot &operator=(PaimonSnapshot &&) = default;
};

// Paimon manifest representation
struct PaimonManifest {
    string file_path;
    string file_format;
    uint64_t length;
    uint32_t spec_id;
    uint32_t content;
};

// Paimon manifest entry
struct PaimonManifestEntry {
    string file_path;
    string file_format;
    uint64_t file_size_in_bytes;
    uint32_t spec_id;
    uint32_t content;
    uint32_t status;
    case_insensitive_map_t<string> partition_values;
};

// Paimon schema field
struct PaimonSchemaField {
    int id;
    string name;
    PaimonDataType type;
    bool nullable = true;
};

// Paimon schema
struct PaimonSchema {
    int id;
    vector<PaimonSchemaField> fields;
    vector<string> partition_keys;  // Names of partition key columns
};

// Helper struct for snapshot metadata parsing
struct SnapshotMetadata {
    timestamp_t timestamp_ms = 0;
    uint64_t snapshot_id = 0;
};

// Paimon table metadata
class PaimonTableMetadata {
public:
    static string GetMetaDataPath(ClientContext &context, const string &table_location, FileSystem &fs,
                                  const PaimonOptions &options);

    static SnapshotMetadata ParseSnapshotMetadata(const string &metadata_path, FileSystem &fs,
                                                  const string &compression_codec);

    static unique_ptr<PaimonTableMetadata> Parse(const string &metadata_path, FileSystem &fs,
                                                 const string &compression_codec);

    // Snapshot lookup methods
    PaimonSnapshot *FindSnapshotByTimestamp(timestamp_t timestamp);
    PaimonSnapshot *FindSnapshotById(uint64_t snapshot_id);
    PaimonSnapshot *GetCurrentSnapshot(const PaimonOptions &options);

    // Schema parsing helpers
    static void ParseSchemaFromJson(yyjson_val *schema_obj, PaimonSchema &schema);
    static void ParseSchemaFieldFromJson(yyjson_val *field_obj, PaimonSchemaField &field);
    static void ParseDataTypeFromJson(yyjson_val *type_obj, PaimonDataType &data_type);
    static PaimonTypeRoot StringToTypeRoot(const string &type_str);
    static void CreateDefaultSchema(PaimonSchema &schema);

    unordered_map<uint64_t, PaimonSnapshot> snapshots;
    case_insensitive_map_t<string> properties;
    string table_format_version;
    unique_ptr<PaimonSchema> schema;
};

// Paimon file format enumeration
enum class PaimonFileFormat {
    PARQUET,
    ORC,
    AVRO
};

// File source enumeration (for DataFileMeta)
enum class FileSource : int8_t {
    APPEND = 0,
    COMPACT = 1
};

// Simple statistics structure for Paimon
struct SimpleStats {
    std::vector<std::string> colNames;
    std::vector<std::vector<Value>> colStats;  // Each column's stats as array of values
};

// Complete DataFileMeta structure matching Paimon DataFileMeta.SCHEMA (20 fields)
struct DataFileMeta {
    // Core file information (fields 0-2)
    std::string fileName;
    int64_t fileSize;
    int64_t rowCount;

    // Key bounds (fields 3-4) - binary encoded
    std::vector<uint8_t> minKey;
    std::vector<uint8_t> maxKey;

    // Statistics (fields 5-6)
    SimpleStats keyStats;
    SimpleStats valueStats;

    // Sequence numbers (fields 7-8)
    int64_t minSequenceNumber;
    int64_t maxSequenceNumber;

    // Schema and level (fields 9-10)
    int64_t schemaId;
    int level;

    // Additional files (field 11)
    std::vector<std::string> extraFiles;

    // Timestamps (field 12)
    timestamp_t creationTime;

    // Delete information (field 13) - nullable
    optional_ptr<int64_t> deleteRowCount;

    // Index information (field 14) - nullable
    optional_ptr<std::vector<uint8_t>> embeddedFileIndex;

    // Source tracking (field 15) - nullable
    optional_ptr<FileSource> fileSource;

    // Column information (fields 16-19)
    std::vector<std::string> valueStatsCols;
    optional_ptr<std::string> externalPath;
    optional_ptr<int64_t> firstRowId;
    optional_ptr<std::vector<std::string>> writeCols;

    // Constructor with defaults
    DataFileMeta() :
        fileSize(0), rowCount(0), minSequenceNumber(0), maxSequenceNumber(0),
        schemaId(0), level(0), creationTime(Timestamp::GetCurrentTimestamp()) {}
};

// BucketManager for deterministic bucket assignment
class BucketManager {
private:
    int numBuckets;
    std::hash<std::string> hasher;

public:
    BucketManager(int numBuckets);

    // Bucket assignment methods
    int assignBucket(const std::string& key) const;
    int assignBucket(const std::vector<Value>& partitionValues, const Value& primaryKey) const;

    // Utility methods
    int getNumBuckets() const { return numBuckets; }
    std::vector<int> getAllBuckets() const;
};

// FileStorePathFactory for Paimon-compliant path management
class FileStorePathFactory {
private:
    std::string tablePath;
    int numBuckets;

public:
    FileStorePathFactory(const std::string& tablePath, int numBuckets = 1);

    // Bucket-based paths
    std::string bucketPath(int bucket) const;
    std::string dataFilePath(int bucket, const std::string& uuid, int counter, PaimonFileFormat format) const;
    std::string deleteFilePath(int bucket, const std::string& uuid, int counter, PaimonFileFormat format) const;

    // Manifest paths (always Avro)
    std::string manifestFilePath(const std::string& uuid, int index) const;
    std::string manifestListFilePath(const std::string& uuid, int index) const;

    // Snapshot paths
    std::string snapshotFilePath(int64_t snapshotId) const;
    std::string earliestPointerPath() const;
    std::string latestPointerPath() const;

    // Partitioned paths
    std::string partitionBucketPath(const std::vector<std::pair<std::string, std::string>>& partition, int bucket) const;
    std::string partitionedDataFilePath(const std::vector<std::pair<std::string, std::string>>& partition, int bucket,
                                       const std::string& uuid, int counter, PaimonFileFormat format) const;
    std::string partitionedDeleteFilePath(const std::vector<std::pair<std::string, std::string>>& partition, int bucket,
                                         const std::string& uuid, int counter, PaimonFileFormat format) const;

    // Utility methods
    int getNumBuckets() const { return numBuckets; }
    std::string getTablePath() const { return tablePath; }
    std::string getFormatExtension(PaimonFileFormat format) const;
};

} // namespace duckdb
