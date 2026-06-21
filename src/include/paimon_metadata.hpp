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
#include "duckdb/common/types/value.hpp"
#include "yyjson.hpp"
#include <memory>
#include <vector>
#include <unordered_map>

using namespace duckdb_yyjson;

namespace duckdb {

// YyjsonDocDeleter for safe memory management of yyjson documents
struct YyjsonDocDeleter {
	void operator()(yyjson_doc *doc) {
		yyjson_doc_free(doc);
	}
	void operator()(yyjson_mut_doc *doc) {
		yyjson_mut_doc_free(doc);
	}
};

class ClientContext;
class FileSystem;
class TableFunctionInput;
class DataChunk;

// Forward declarations
struct PaimonSnapshot;
struct PaimonManifest;
struct PaimonManifestEntry;
struct PaimonTableMetadata;

//===--------------------------------------------------------------------===//
// Paimon Type System
//===--------------------------------------------------------------------===//

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

// Convert PaimonTypeRoot to string
string PaimonTypeRootToString(PaimonTypeRoot type_root);

struct PaimonSchemaField;

struct PaimonDataType {
	PaimonTypeRoot type_root = PaimonTypeRoot::STRING;
	int precision = -1;                          // For DECIMAL
	int scale = -1;                              // For DECIMAL
	unique_ptr<PaimonDataType> element_type;     // For ARRAY
	unique_ptr<PaimonDataType> key_type;         // For MAP
	unique_ptr<PaimonDataType> value_type;       // For MAP
	vector<PaimonSchemaField> fields;            // For STRUCT

	PaimonDataType() = default;
	PaimonDataType(const PaimonDataType &other);
	PaimonDataType &operator=(const PaimonDataType &other);
	PaimonDataType(PaimonDataType &&) = default;
	PaimonDataType &operator=(PaimonDataType &&) = default;

	string ToString() const;
};

// Convert PaimonDataType to DuckDB LogicalType
LogicalType PaimonTypeToLogicalType(const PaimonDataType &paimon_type);

//===--------------------------------------------------------------------===//
// Paimon Schema
//===--------------------------------------------------------------------===//

struct PaimonSchemaField {
	int id = 0;
	string name;
	PaimonDataType type;
	bool nullable = true;
};

struct PaimonSchema {
	int id = 0;
	vector<PaimonSchemaField> fields;
	vector<string> partition_keys;
	vector<string> primary_keys;
	case_insensitive_map_t<string> options;
};

//===--------------------------------------------------------------------===//
// Paimon Options
//===--------------------------------------------------------------------===//

struct PaimonOptions {
	string metadata_compression_codec = "gzip";
	string table_version = "latest";
	string version_name_format = "v%s%s";

	//! Incremental (changelog) read: return the records written in the delta of snapshots
	//! (incremental_from, incremental_to]. When enabled, no merge-on-read is applied — the raw
	//! changelog records of that snapshot range are returned.
	bool incremental = false;
	uint64_t incremental_from = 0; // exclusive
	uint64_t incremental_to = 0;   // inclusive

	struct SnapshotLookup {
		enum class SnapshotSource { LATEST, FROM_ID, FROM_TIMESTAMP };
		SnapshotSource snapshot_source = SnapshotSource::LATEST;
		uint64_t snapshot_id = 0;
		timestamp_t snapshot_timestamp;
	} snapshot_lookup;
};

//===--------------------------------------------------------------------===//
// Paimon Snapshot (Version 3 format)
//===--------------------------------------------------------------------===//

struct PaimonSnapshot {
	int version = 3;
	uint64_t snapshot_id = 0;
	int64_t schema_id = 0;

	// Manifest references
	string base_manifest_list;
	string delta_manifest_list;
	string changelog_manifest_list_str;   // nullable
	string index_manifest_str;            // nullable

	// Commit metadata
	string commit_user;
	int64_t commit_identifier = 0;
	string commit_kind;
	int64_t timestamp_ms = 0;
	string log_offsets;

	// Record counts (nullable — use has_* flags)
	int64_t total_record_count_val = 0;
	bool has_total_record_count = false;
	int64_t delta_record_count_val = 0;
	bool has_delta_record_count = false;
	int64_t changelog_record_count_val = 0;
	bool has_changelog_record_count = false;

	// Watermark (nullable)
	int64_t watermark_val = 0;
	bool has_watermark = false;

	// Legacy/compatibility fields
	uint64_t sequence_number = 0;
	string manifest_list;

	// Convenience: get the timestamp as a DuckDB timestamp
	timestamp_t GetTimestamp() const {
		return timestamp_t(timestamp_ms * 1000); // ms to us
	}
};

//===--------------------------------------------------------------------===//
// Paimon Manifest Structures
//===--------------------------------------------------------------------===//

struct PaimonManifest {
	string file_path;
	string file_format;
	uint64_t length = 0;
	uint32_t spec_id = 0;
	uint32_t content = 0;
};

struct PaimonManifestEntry {
	string file_path;
	string file_format;
	uint64_t file_size_in_bytes = 0;
	uint32_t spec_id = 0;
	uint32_t content = 0;
	uint32_t status = 0;    // 0=ADD, 1=DELETE
	int32_t bucket = 0;
	case_insensitive_map_t<string> partition_values;
};

//===--------------------------------------------------------------------===//
// Helper Structs
//===--------------------------------------------------------------------===//

struct SnapshotMetadata {
	int64_t timestamp_ms = 0;
	uint64_t snapshot_id = 0;
};

//===--------------------------------------------------------------------===//
// File Format / Source Enums
//===--------------------------------------------------------------------===//

enum class PaimonFileFormat {
	PARQUET,
	ORC,
	AVRO
};

enum class FileSource : int8_t {
	APPEND = 0,
	COMPACT = 1
};

// Convert PaimonFileFormat to string
string PaimonFileFormatToString(PaimonFileFormat format);

// Convert FileSource to string
string FileSourceToString(FileSource source);

//===--------------------------------------------------------------------===//
// Statistics
//===--------------------------------------------------------------------===//

struct SimpleStats {
	std::vector<std::string> colNames;
	std::vector<std::vector<Value>> colStats;
};

struct DataFileMeta {
	std::string fileName;
	int64_t fileSize = 0;
	int64_t rowCount = 0;

	std::vector<uint8_t> minKey;
	std::vector<uint8_t> maxKey;

	SimpleStats keyStats;
	SimpleStats valueStats;

	int64_t minSequenceNumber = 0;
	int64_t maxSequenceNumber = 0;

	int64_t schemaId = 0;
	int level = 0;

	std::vector<std::string> extraFiles;
	int64_t creationTime = 0;

	DataFileMeta() = default;
};

//===--------------------------------------------------------------------===//
// PaimonTableMetadata
//===--------------------------------------------------------------------===//

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

	// Schema parsing helpers (kept for compatibility)
	static void ParseSchemaFromJson(yyjson_val *schema_obj, PaimonSchema &schema);
	static void ParseSchemaFieldFromJson(yyjson_val *field_obj, PaimonSchemaField &field);
	static void ParseDataTypeFromJson(yyjson_val *type_obj, PaimonDataType &data_type);
	static PaimonTypeRoot StringToTypeRoot(const string &type_str);
	static void CreateDefaultSchema(PaimonSchema &schema);

	unordered_map<uint64_t, PaimonSnapshot> snapshots;
	case_insensitive_map_t<string> properties;
	string table_format_version;
	string table_location;
	unique_ptr<PaimonSchema> schema;
};

//===--------------------------------------------------------------------===//
// BucketManager
//===--------------------------------------------------------------------===//

class BucketManager {
private:
	int numBuckets;
	std::hash<std::string> hasher;

public:
	BucketManager(int numBuckets);
	int assignBucket(const std::string &key) const;
	int assignBucket(const std::vector<Value> &partitionValues, const Value &primaryKey) const;
	int getNumBuckets() const {
		return numBuckets;
	}
	std::vector<int> getAllBuckets() const;
};

//===--------------------------------------------------------------------===//
// FileStorePathFactory
//===--------------------------------------------------------------------===//

class FileStorePathFactory {
private:
	std::string tablePath;
	int numBuckets;

public:
	FileStorePathFactory(const std::string &tablePath, int numBuckets = 1);

	std::string bucketPath(int bucket) const;
	std::string dataFilePath(int bucket, const std::string &uuid, int counter, PaimonFileFormat format) const;
	std::string deleteFilePath(int bucket, const std::string &uuid, int counter, PaimonFileFormat format) const;

	std::string manifestFilePath(const std::string &uuid, int index) const;
	std::string manifestListFilePath(const std::string &uuid, int index) const;

	std::string snapshotFilePath(int64_t snapshotId) const;
	std::string earliestPointerPath() const;
	std::string latestPointerPath() const;

	std::string partitionBucketPath(const std::vector<std::pair<std::string, std::string>> &partition,
	                                int bucket) const;
	std::string partitionedDataFilePath(const std::vector<std::pair<std::string, std::string>> &partition, int bucket,
	                                    const std::string &uuid, int counter, PaimonFileFormat format) const;
	std::string partitionedDeleteFilePath(const std::vector<std::pair<std::string, std::string>> &partition, int bucket,
	                                      const std::string &uuid, int counter, PaimonFileFormat format) const;

	int getNumBuckets() const {
		return numBuckets;
	}
	std::string getTablePath() const {
		return tablePath;
	}
	std::string getFormatExtension(PaimonFileFormat format) const;
};

} // namespace duckdb
