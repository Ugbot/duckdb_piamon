#include "paimon_manifest.hpp"
#include "paimon_avro_reader.hpp"
#include "paimon_binary_row.hpp"
#include "paimon_constants.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"

#include <functional>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helper: find column index by name in a query result
//===--------------------------------------------------------------------===//

static idx_t FindColumn(const vector<string> &names, const string &target) {
	for (idx_t i = 0; i < names.size(); i++) {
		if (StringUtil::CIEquals(names[i], target)) {
			return i;
		}
	}
	return DConstants::INVALID_INDEX;
}

//===--------------------------------------------------------------------===//
// Read Paimon Manifest List (Avro file → ManifestFileMeta entries)
//===--------------------------------------------------------------------===//

vector<PaimonManifestFileMeta> ReadPaimonManifestList(ClientContext &context, const string &manifest_list_path) {
	vector<PaimonManifestFileMeta> result;

	// Read the manifest-list Avro file with the native reader (no read_avro; supports zstandard).
	PaimonAvroReader reader(context, manifest_list_path);
	auto &names = reader.GetNames();

	idx_t file_name_idx = FindColumn(names, "_FILE_NAME");
	idx_t file_size_idx = FindColumn(names, "_FILE_SIZE");
	idx_t num_added_idx = FindColumn(names, "_NUM_ADDED_FILES");
	idx_t num_deleted_idx = FindColumn(names, "_NUM_DELETED_FILES");
	idx_t schema_id_idx = FindColumn(names, "_SCHEMA_ID");

	if (file_name_idx == DConstants::INVALID_INDEX) {
		throw IOException("Paimon manifest list missing required column _FILE_NAME");
	}

	for (idx_t row = 0; row < reader.RowCount(); row++) {
		PaimonManifestFileMeta meta;
		meta.file_name = reader.GetValue(row, file_name_idx).ToString();
		if (file_size_idx != DConstants::INVALID_INDEX && !reader.GetValue(row, file_size_idx).IsNull()) {
			meta.file_size = reader.GetValue(row, file_size_idx).GetValue<int64_t>();
		}
		if (num_added_idx != DConstants::INVALID_INDEX && !reader.GetValue(row, num_added_idx).IsNull()) {
			meta.num_added_files = reader.GetValue(row, num_added_idx).GetValue<int64_t>();
		}
		if (num_deleted_idx != DConstants::INVALID_INDEX && !reader.GetValue(row, num_deleted_idx).IsNull()) {
			meta.num_deleted_files = reader.GetValue(row, num_deleted_idx).GetValue<int64_t>();
		}
		if (schema_id_idx != DConstants::INVALID_INDEX && !reader.GetValue(row, schema_id_idx).IsNull()) {
			meta.schema_id = reader.GetValue(row, schema_id_idx).GetValue<int64_t>();
		}
		result.push_back(std::move(meta));
	}

	return result;
}

//===--------------------------------------------------------------------===//
// Read Paimon Manifest File (Avro file → ManifestEntry entries)
//===--------------------------------------------------------------------===//

vector<PaimonManifestEntryParsed> ReadPaimonManifestFile(ClientContext &context, const string &manifest_file_path) {
	vector<PaimonManifestEntryParsed> result;

	// Read the manifest Avro file with the native reader (no read_avro; supports zstandard).
	PaimonAvroReader reader(context, manifest_file_path);
	auto &names = reader.GetNames();

	idx_t kind_idx = FindColumn(names, "_KIND");
	idx_t bucket_idx = FindColumn(names, "_BUCKET");
	idx_t total_buckets_idx = FindColumn(names, "_TOTAL_BUCKETS");
	idx_t partition_idx = FindColumn(names, "_PARTITION");
	idx_t file_idx = FindColumn(names, "_FILE");

	if (kind_idx == DConstants::INVALID_INDEX || file_idx == DConstants::INVALID_INDEX) {
		throw IOException("Paimon manifest file missing required columns (_KIND, _FILE)");
	}

	for (idx_t row = 0; row < reader.RowCount(); row++) {
		PaimonManifestEntryParsed entry;

		auto kind_val = reader.GetValue(row, kind_idx);
		if (!kind_val.IsNull()) {
			entry.kind = static_cast<PaimonFileKind>(kind_val.GetValue<int8_t>());
		}
		if (bucket_idx != DConstants::INVALID_INDEX && !reader.GetValue(row, bucket_idx).IsNull()) {
			entry.bucket = reader.GetValue(row, bucket_idx).GetValue<int32_t>();
		}
		if (total_buckets_idx != DConstants::INVALID_INDEX && !reader.GetValue(row, total_buckets_idx).IsNull()) {
			entry.total_buckets = reader.GetValue(row, total_buckets_idx).GetValue<int32_t>();
		}
		// _PARTITION: raw BinaryRow bytes (BLOB), decoded later against the partition keys.
		if (partition_idx != DConstants::INVALID_INDEX && !reader.GetValue(row, partition_idx).IsNull()) {
			entry.partition = StringValue::Get(reader.GetValue(row, partition_idx));
		}

		// _FILE is a nested STRUCT — extract the fields we need.
		auto file_val = reader.GetValue(row, file_idx);
		if (!file_val.IsNull() && file_val.type().id() == LogicalTypeId::STRUCT) {
			auto &children = StructValue::GetChildren(file_val);
			auto &child_types = StructType::GetChildTypes(file_val.type());
			for (idx_t c = 0; c < child_types.size(); c++) {
				auto &child_name = child_types[c].first;
				auto &child_val = children[c];
				if (child_val.IsNull()) {
					continue;
				}
				if (StringUtil::CIEquals(child_name, "_FILE_NAME")) {
					entry.file.file_name = child_val.ToString();
				} else if (StringUtil::CIEquals(child_name, "_FILE_SIZE")) {
					entry.file.file_size = child_val.GetValue<int64_t>();
				} else if (StringUtil::CIEquals(child_name, "_ROW_COUNT")) {
					entry.file.row_count = child_val.GetValue<int64_t>();
				} else if (StringUtil::CIEquals(child_name, "_SCHEMA_ID")) {
					entry.file.schema_id = child_val.GetValue<int64_t>();
				} else if (StringUtil::CIEquals(child_name, "_LEVEL")) {
					entry.file.level = child_val.GetValue<int32_t>();
				}
			}
		}
		result.push_back(std::move(entry));
	}

	return result;
}

//===--------------------------------------------------------------------===//
// Compute Active Data Files from Manifests
//===--------------------------------------------------------------------===//

// A data file's identity within a table: (bucket, file name, LSM level).
namespace {
struct ActiveFileId {
	int32_t bucket;
	string file_name;
	int32_t level;
	bool operator==(const ActiveFileId &o) const {
		return bucket == o.bucket && file_name == o.file_name && level == o.level;
	}
};
struct ActiveFileIdHash {
	size_t operator()(const ActiveFileId &id) const {
		size_t h = std::hash<int32_t>()(id.bucket);
		h ^= std::hash<string>()(id.file_name) + 0x9e3779b9 + (h << 6) + (h >> 2);
		h ^= std::hash<int32_t>()(id.level) + 0x9e3779b9 + (h << 6) + (h >> 2);
		return h;
	}
};
} // namespace

// Resolves a path within the table, falling back to the main table root for a branch (see
// ComputeActiveDataFiles). Identity for a non-branch table.
using PathResolver = std::function<string(const string &)>;

// Read every manifest entry referenced by a snapshot's base + delta manifest lists (base first, the
// accumulated state; then the delta).
static vector<PaimonManifestEntryParsed> CollectManifestEntries(ClientContext &context, const string &table_location,
                                                                FileSystem &fs, const PathResolver &resolve,
                                                                const string &base_list, const string &delta_list) {
	vector<string> manifest_file_paths;
	auto read_manifest_list = [&](const string &ref) {
		if (ref.empty()) {
			return;
		}
		string list_path = (StringUtil::StartsWith(ref, "/") || ref.find("://") != string::npos)
		                        ? ref
		                        : resolve(table_location + "/manifest/" + ref);
		if (!fs.FileExists(list_path)) {
			return;
		}
		for (auto &meta : ReadPaimonManifestList(context, list_path)) {
			bool absolute = StringUtil::StartsWith(meta.file_name, "/") || meta.file_name.find("://") != string::npos;
			manifest_file_paths.push_back(absolute ? meta.file_name
			                                       : resolve(table_location + "/manifest/" + meta.file_name));
		}
	};
	read_manifest_list(base_list);
	read_manifest_list(delta_list);

	vector<PaimonManifestEntryParsed> all_entries;
	for (auto &manifest_path : manifest_file_paths) {
		if (!fs.FileExists(manifest_path)) {
			continue;
		}
		for (auto &entry : ReadPaimonManifestFile(context, manifest_path)) {
			all_entries.push_back(std::move(entry));
		}
	}
	return all_entries;
}

// The net-active file set: an ADD entry adds a file, a later DELETE for the same identity removes it.
static vector<PaimonManifestEntryParsed> ComputeActiveSet(const vector<PaimonManifestEntryParsed> &all_entries) {
	unordered_map<ActiveFileId, PaimonManifestEntryParsed, ActiveFileIdHash> active_files;
	for (auto &entry : all_entries) {
		// Bucket and LSM level are physical, non-negative identifiers; a negative value would mean the
		// manifest decode produced garbage.
		D_ASSERT(entry.bucket >= 0);
		D_ASSERT(entry.file.level >= 0);
		ActiveFileId id {entry.bucket, entry.file.file_name, entry.file.level};
		if (entry.kind == PaimonFileKind::ADD) {
			active_files[id] = entry;
		} else {
			active_files.erase(id);
		}
	}
	vector<PaimonManifestEntryParsed> result;
	result.reserve(active_files.size());
	for (auto &kv : active_files) {
		result.push_back(kv.second);
	}
	return result;
}

// Resolve each active entry to an absolute data-file path: {table}/[partition=.../]bucket-N/<file>,
// reconstructing the Hive-style partition directory from the manifest's _PARTITION BinaryRow.
static vector<string> ResolveDataFilePaths(const vector<PaimonManifestEntryParsed> &active,
                                           const string &table_location, const vector<string> &partition_keys,
                                           const vector<PaimonDataType> &partition_types,
                                           const PathResolver &resolve) {
	auto partition_dir = [&](const string &partition_bytes) -> string {
		if (partition_keys.empty() || partition_bytes.empty()) {
			return "";
		}
		auto values = PaimonBinaryRow::DecodeSerialized(partition_bytes, partition_types);
		if (values.size() != partition_keys.size()) {
			return "";
		}
		string dir;
		for (idx_t i = 0; i < partition_keys.size(); i++) {
			// Paimon names a null partition value with partition.default-name.
			string v = values[i].IsNull() ? paimon::DEFAULT_PARTITION_NAME : values[i].ToString();
			dir += partition_keys[i] + "=" + v + "/";
		}
		return dir;
	};

	vector<string> result;
	result.reserve(active.size());
	for (auto &entry : active) {
		const string &name = entry.file.file_name;
		if (StringUtil::StartsWith(name, "/") || name.find("://") != string::npos) {
			result.push_back(name); // already absolute
		} else if (name.find("bucket-") != string::npos || name.find("/") != string::npos) {
			result.push_back(resolve(table_location + "/" + name)); // already has directory structure
		} else {
			result.push_back(resolve(table_location + "/" + partition_dir(entry.partition) + "bucket-" +
			                         std::to_string(entry.bucket) + "/" + name)); // bare filename
		}
	}
	return result;
}

vector<string> ComputeActiveDataFiles(ClientContext &context, const string &table_location,
                                      const string &base_manifest_list, const string &delta_manifest_list,
                                      const PaimonSchema *schema) {
	FileSystem &fs = FileSystem::GetFileSystem(context);

	// Branch support: a named branch (table_location ends in .../branch/branch-<name>) keeps its own
	// snapshot + schema but SHARES the main table's manifest and data files, matching Paimon's
	// branch-aware path resolution. Resolve each path within the branch dir first, falling back to the
	// equivalent path under the main table root when it is not present there.
	string main_root = table_location;
	auto branch_pos = table_location.find("/branch/branch-");
	if (branch_pos != string::npos) {
		main_root = table_location.substr(0, branch_pos);
	}
	PathResolver resolve = [&fs, main_root, table_location](const string &p) -> string {
		if (main_root == table_location || fs.FileExists(p)) {
			return p; // not a branch, or the branch already has its own copy
		}
		if (StringUtil::StartsWith(p, table_location)) {
			string rebased = main_root + p.substr(table_location.size());
			if (fs.FileExists(rebased)) {
				return rebased;
			}
		}
		return p;
	};

	// Partition-key field types (in partition-key order), used to decode the _PARTITION BinaryRow.
	vector<string> partition_keys;
	vector<PaimonDataType> partition_types;
	if (schema) {
		for (auto &pk : schema->partition_keys) {
			for (auto &field : schema->fields) {
				if (field.name == pk) {
					partition_keys.push_back(field.name);
					partition_types.push_back(field.type);
					break;
				}
			}
		}
	}

	auto all_entries =
	    CollectManifestEntries(context, table_location, fs, resolve, base_manifest_list, delta_manifest_list);
	auto active = ComputeActiveSet(all_entries);
	return ResolveDataFilePaths(active, table_location, partition_keys, partition_types, resolve);
}

} // namespace duckdb
