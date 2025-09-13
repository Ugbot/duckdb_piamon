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

// Paimon snapshot representation
struct PaimonSnapshot {
    uint64_t snapshot_id = 0;
    uint64_t sequence_number = 0;
    timestamp_t timestamp_ms;
    string manifest_list;
    string schema_id;
    string base_manifest_list;
    case_insensitive_map_t<string> summary;

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
};

// Paimon table metadata
class PaimonTableMetadata {
public:
    static string GetMetaDataPath(ClientContext &context, const string &table_location, FileSystem &fs,
                                  const PaimonOptions &options);

    static unique_ptr<PaimonTableMetadata> Parse(const string &metadata_path, FileSystem &fs,
                                                 const string &compression_codec);

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

} // namespace duckdb
