#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "paimon_metadata.hpp"

namespace duckdb {

class ClientContext;
struct PaimonSchema;

//===--------------------------------------------------------------------===//
// Paimon Manifest File Meta (rows in a manifest-list Avro file)
//===--------------------------------------------------------------------===//

struct PaimonManifestFileMeta {
	string file_name;
	int64_t file_size = 0;
	int64_t num_added_files = 0;
	int64_t num_deleted_files = 0;
	int64_t schema_id = 0;
};

//===--------------------------------------------------------------------===//
// Paimon Manifest Entry (rows in a manifest Avro file)
//===--------------------------------------------------------------------===//

enum class PaimonFileKind : int8_t {
	ADD = 0,
	DELETE = 1,
};

struct PaimonDataFileInfo {
	string file_name;
	int64_t file_size = 0;
	int64_t row_count = 0;
	int64_t schema_id = 0;
	int32_t level = 0;
};

struct PaimonManifestEntryParsed {
	PaimonFileKind kind = PaimonFileKind::ADD;
	int32_t bucket = 0;
	int32_t total_buckets = 0;
	PaimonDataFileInfo file;
	//! Raw _PARTITION BinaryRow bytes (empty for unpartitioned tables). Decoded against the table's
	//! partition keys to reconstruct the Hive-style partition directory (key=value/...).
	string partition;
};

//===--------------------------------------------------------------------===//
// Paimon Manifest Reader Functions
//===--------------------------------------------------------------------===//

//! Read a manifest-list Avro file and return the list of manifest file metas
vector<PaimonManifestFileMeta> ReadPaimonManifestList(ClientContext &context, const string &manifest_list_path);

//! Read a manifest Avro file and return the list of manifest entries
vector<PaimonManifestEntryParsed> ReadPaimonManifestFile(ClientContext &context, const string &manifest_file_path);

//! Given a snapshot's manifest lists, compute the set of active data file paths.
//! table_location is the root table path used to resolve relative file paths. `schema` (may be null)
//! supplies the partition keys/types needed to reconstruct partition directories from the manifest
//! _PARTITION BinaryRow.
//! Returns absolute paths to the active data files.
vector<string> ComputeActiveDataFiles(ClientContext &context, const string &table_location,
                                      const string &base_manifest_list, const string &delta_manifest_list,
                                      const PaimonSchema *schema = nullptr);

} // namespace duckdb
