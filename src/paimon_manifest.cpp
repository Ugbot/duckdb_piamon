#include "paimon_manifest.hpp"
#include "paimon_avro_scan.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/file_system.hpp"

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

	// Read the Avro file natively via the read_avro table function (no nested SQL — see PaimonAvroScan).
	PaimonAvroScan scan("paimon_manifest_list", context, manifest_list_path);
	auto &names = scan.GetNames();

	// Map column names to indices
	idx_t file_name_idx = FindColumn(names, "_FILE_NAME");
	idx_t file_size_idx = FindColumn(names, "_FILE_SIZE");
	idx_t num_added_idx = FindColumn(names, "_NUM_ADDED_FILES");
	idx_t num_deleted_idx = FindColumn(names, "_NUM_DELETED_FILES");
	idx_t schema_id_idx = FindColumn(names, "_SCHEMA_ID");

	if (file_name_idx == DConstants::INVALID_INDEX) {
		throw IOException("Paimon manifest list missing required column _FILE_NAME");
	}

	// Fetch all chunks
	DataChunk chunk_storage;
	scan.InitializeChunk(chunk_storage);
	while (true) {
		chunk_storage.Reset();
		if (!scan.GetNext(chunk_storage)) {
			break;
		}
		DataChunk *chunk = &chunk_storage;
		for (idx_t row = 0; row < chunk->size(); row++) {
			PaimonManifestFileMeta meta;
			meta.file_name = chunk->GetValue(file_name_idx, row).ToString();

			if (file_size_idx != DConstants::INVALID_INDEX) {
				auto val = chunk->GetValue(file_size_idx, row);
				if (!val.IsNull()) {
					meta.file_size = val.GetValue<int64_t>();
				}
			}
			if (num_added_idx != DConstants::INVALID_INDEX) {
				auto val = chunk->GetValue(num_added_idx, row);
				if (!val.IsNull()) {
					meta.num_added_files = val.GetValue<int64_t>();
				}
			}
			if (num_deleted_idx != DConstants::INVALID_INDEX) {
				auto val = chunk->GetValue(num_deleted_idx, row);
				if (!val.IsNull()) {
					meta.num_deleted_files = val.GetValue<int64_t>();
				}
			}
			if (schema_id_idx != DConstants::INVALID_INDEX) {
				auto val = chunk->GetValue(schema_id_idx, row);
				if (!val.IsNull()) {
					meta.schema_id = val.GetValue<int64_t>();
				}
			}

			result.push_back(std::move(meta));
		}
	}

	return result;
}

//===--------------------------------------------------------------------===//
// Read Paimon Manifest File (Avro file → ManifestEntry entries)
//===--------------------------------------------------------------------===//

vector<PaimonManifestEntryParsed> ReadPaimonManifestFile(ClientContext &context, const string &manifest_file_path) {
	vector<PaimonManifestEntryParsed> result;

	// Read the Avro file natively via the read_avro table function (no nested SQL — see PaimonAvroScan).
	PaimonAvroScan scan("paimon_manifest_file", context, manifest_file_path);
	auto &names = scan.GetNames();

	// Map column names to indices
	idx_t kind_idx = FindColumn(names, "_KIND");
	idx_t bucket_idx = FindColumn(names, "_BUCKET");
	idx_t total_buckets_idx = FindColumn(names, "_TOTAL_BUCKETS");
	idx_t file_idx = FindColumn(names, "_FILE");

	if (kind_idx == DConstants::INVALID_INDEX || file_idx == DConstants::INVALID_INDEX) {
		throw IOException("Paimon manifest file missing required columns (_KIND, _FILE)");
	}

	// Fetch all chunks
	DataChunk chunk_storage;
	scan.InitializeChunk(chunk_storage);
	while (true) {
		chunk_storage.Reset();
		if (!scan.GetNext(chunk_storage)) {
			break;
		}
		DataChunk *chunk = &chunk_storage;
		for (idx_t row = 0; row < chunk->size(); row++) {
			PaimonManifestEntryParsed entry;

			// _KIND: 0=ADD, 1=DELETE
			auto kind_val = chunk->GetValue(kind_idx, row);
			if (!kind_val.IsNull()) {
				entry.kind = static_cast<PaimonFileKind>(kind_val.GetValue<int8_t>());
			}

			// _BUCKET
			if (bucket_idx != DConstants::INVALID_INDEX) {
				auto val = chunk->GetValue(bucket_idx, row);
				if (!val.IsNull()) {
					entry.bucket = val.GetValue<int32_t>();
				}
			}

			// _TOTAL_BUCKETS
			if (total_buckets_idx != DConstants::INVALID_INDEX) {
				auto val = chunk->GetValue(total_buckets_idx, row);
				if (!val.IsNull()) {
					entry.total_buckets = val.GetValue<int32_t>();
				}
			}

			// _FILE is a nested STRUCT — extract the fields we need
			auto file_val = chunk->GetValue(file_idx, row);
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
	}

	return result;
}

//===--------------------------------------------------------------------===//
// Compute Active Data Files from Manifests
//===--------------------------------------------------------------------===//

vector<string> ComputeActiveDataFiles(ClientContext &context, const string &table_location,
                                      const string &base_manifest_list, const string &delta_manifest_list) {
	FileSystem &fs = FileSystem::GetFileSystem(context);

	// Collect all manifest file paths from both base and delta manifest lists
	vector<string> manifest_file_paths;

	auto read_manifest_list = [&](const string &manifest_list_ref) {
		if (manifest_list_ref.empty()) {
			return;
		}

		// Resolve the manifest list path (could be relative to manifest/ directory)
		string manifest_list_path;
		if (StringUtil::StartsWith(manifest_list_ref, "/") || manifest_list_ref.find("://") != string::npos) {
			manifest_list_path = manifest_list_ref;
		} else {
			manifest_list_path = table_location + "/manifest/" + manifest_list_ref;
		}

		if (!fs.FileExists(manifest_list_path)) {
			return;
		}

		auto metas = ReadPaimonManifestList(context, manifest_list_path);
		for (auto &meta : metas) {
			// Resolve manifest file path
			string manifest_path;
			if (StringUtil::StartsWith(meta.file_name, "/") || meta.file_name.find("://") != string::npos) {
				manifest_path = meta.file_name;
			} else {
				manifest_path = table_location + "/manifest/" + meta.file_name;
			}
			manifest_file_paths.push_back(manifest_path);
		}
	};

	// Read base manifest list first (accumulated state), then delta (this snapshot's additions)
	read_manifest_list(base_manifest_list);
	read_manifest_list(delta_manifest_list);

	// Read all manifest files and collect entries
	vector<PaimonManifestEntryParsed> all_entries;
	for (auto &manifest_path : manifest_file_paths) {
		if (!fs.FileExists(manifest_path)) {
			continue;
		}
		auto entries = ReadPaimonManifestFile(context, manifest_path);
		for (auto &entry : entries) {
			all_entries.push_back(std::move(entry));
		}
	}

	// Merge ADD/DELETE entries to compute active file set
	// Key: (bucket, file_name) → entry
	// Process in order: ADD puts file in set, DELETE removes it
	struct FileIdentifier {
		int32_t bucket;
		string file_name;
		int32_t level;

		bool operator==(const FileIdentifier &other) const {
			return bucket == other.bucket && file_name == other.file_name && level == other.level;
		}
	};

	struct FileIdentifierHash {
		size_t operator()(const FileIdentifier &id) const {
			size_t h = std::hash<int32_t>()(id.bucket);
			h ^= std::hash<string>()(id.file_name) + 0x9e3779b9 + (h << 6) + (h >> 2);
			h ^= std::hash<int32_t>()(id.level) + 0x9e3779b9 + (h << 6) + (h >> 2);
			return h;
		}
	};

	unordered_map<FileIdentifier, PaimonManifestEntryParsed, FileIdentifierHash> active_files;

	for (auto &entry : all_entries) {
		FileIdentifier id{entry.bucket, entry.file.file_name, entry.file.level};

		if (entry.kind == PaimonFileKind::ADD) {
			active_files[id] = entry;
		} else {
			// DELETE cancels a previous ADD
			active_files.erase(id);
		}
	}

	// Convert active files to absolute paths
	vector<string> result;
	result.reserve(active_files.size());

	for (auto &kv : active_files) {
		auto &entry = kv.second;
		string file_path;

		// The file_name in a manifest entry is relative to the bucket directory
		// Full path: {table_location}/bucket-{N}/{file_name}
		// Or for partitioned: {table_location}/{partition_path}/bucket-{N}/{file_name}
		// The file_name field might already include the bucket prefix or be just the filename

		if (StringUtil::StartsWith(entry.file.file_name, "/") ||
		    entry.file.file_name.find("://") != string::npos) {
			// Absolute path
			file_path = entry.file.file_name;
		} else if (entry.file.file_name.find("bucket-") != string::npos ||
		           entry.file.file_name.find("/") != string::npos) {
			// Already has directory structure (e.g., "bucket-0/data-xxx.parquet" or "dt=.../bucket-0/...")
			file_path = table_location + "/" + entry.file.file_name;
		} else {
			// Just a filename — put it under the bucket directory
			file_path = table_location + "/bucket-" + std::to_string(entry.bucket) + "/" + entry.file.file_name;
		}

		result.push_back(file_path);
	}

	return result;
}

} // namespace duckdb
