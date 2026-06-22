#include "paimon_multi_file_list.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "paimon_metadata.hpp"
#include "paimon_manifest.hpp"
#include "iceberg_utils.hpp"

namespace duckdb {

PaimonMultiFileList::PaimonMultiFileList(ClientContext &context, const string &path)
    : MultiFileList(vector<OpenFileInfo> {}, FileGlobOptions::ALLOW_EMPTY), context(context), path(path) {
	DiscoverDataFiles();
}

PaimonMultiFileList::PaimonMultiFileList(ClientContext &context, const string &path, const PaimonOptions &options)
    : MultiFileList(vector<OpenFileInfo> {}, FileGlobOptions::ALLOW_EMPTY), context(context), path(path),
      scan_options(options) {
	DiscoverDataFiles();
}

PaimonMultiFileList::PaimonMultiFileList(ClientContext &context, const string &path, const vector<string> &files)
    : MultiFileList(vector<OpenFileInfo> {}, FileGlobOptions::ALLOW_EMPTY), context(context), path(path),
      files(files) {
}

//===--------------------------------------------------------------------===//
// File Discovery
//===--------------------------------------------------------------------===//

void PaimonMultiFileList::DiscoverDataFiles() {
	FileSystem &fs = FileSystem::GetFileSystem(context);

	// Step 1: Load metadata (snapshot + schema) for the requested snapshot (latest / id / timestamp).
	try {
		auto paimon_meta_path = PaimonTableMetadata::GetMetaDataPath(context, path, fs, scan_options);
		metadata = PaimonTableMetadata::Parse(paimon_meta_path, fs, scan_options.metadata_compression_codec);
	} catch (const std::exception &e) {
		// Metadata loading failed — still try to discover files directly
	}

	// Incremental (changelog) read: union the delta data files of snapshots (from, to].
	if (scan_options.incremental && metadata) {
		const PaimonSchema *schema = metadata->schema.get();
		for (uint64_t s = scan_options.incremental_from + 1; s <= scan_options.incremental_to; s++) {
			string snap_path = path + "/snapshot/snapshot-" + std::to_string(s);
			if (!fs.FileExists(snap_path)) {
				continue;
			}
			try {
				string json = IcebergUtils::FileToString(snap_path, fs);
				auto doc =
				    unique_ptr<yyjson_doc, YyjsonDocDeleter>(yyjson_read(json.c_str(), json.size(), 0));
				if (!doc) {
					continue;
				}
				auto v = yyjson_obj_get(yyjson_doc_get_root(doc.get()), "deltaManifestList");
				if (v && yyjson_is_str(v)) {
					string delta_list = yyjson_get_str(v);
					if (!delta_list.empty()) {
						auto delta_files = ComputeActiveDataFiles(context, path, "", delta_list, schema);
						for (auto &f : delta_files) {
							files.push_back(f);
						}
					}
				}
			} catch (const std::exception &e) {
				// Skip snapshots we can't read.
			}
		}
		return;
	}

	// Step 2: Try manifest-based file discovery (correct approach)
	if (metadata) {
		auto *snapshot = metadata->GetCurrentSnapshot(scan_options);
		if (snapshot) {
			try {
				files = ComputeActiveDataFiles(context, path, snapshot->base_manifest_list,
				                               snapshot->delta_manifest_list, metadata->schema.get());
				// Validate that every resolved file actually exists. For partitioned tables the
				// partition directory (e.g. dt=.../) is encoded in the manifest's _PARTITION BinaryRow,
				// which is not yet decoded — so resolved paths can be wrong. In that case fall back to
				// directory discovery rather than failing the scan. (Proper fix: Phase 2 BinaryRow decode.)
				bool all_exist = !files.empty();
				for (auto &f : files) {
					if (!fs.FileExists(f)) {
						all_exist = false;
						break;
					}
				}
				if (all_exist) {
					return; // Manifest-based discovery succeeded
				}
				files.clear();
			} catch (const std::exception &e) {
				// Manifest reading failed — fall through to directory scanning
			}
		}
	}

	// Step 3: Fallback — discover files from directory structure (BFS)
	// Paimon layout: {table}/[partition_key=value/]bucket-N/data-*.parquet
	DiscoverFilesFromBucketDirs(path, fs);

	// If no files found at root level, check inside a data/ directory
	if (files.empty()) {
		string data_dir = path + "/data";
		if (fs.DirectoryExists(data_dir)) {
			DiscoverFilesFromBucketDirs(data_dir, fs);
			fs.ListFiles(data_dir, [&](const string &fname, bool is_dir) {
				if (!is_dir && (StringUtil::EndsWith(fname, ".parquet") || StringUtil::EndsWith(fname, ".orc"))) {
					files.push_back(data_dir + "/" + fname);
				}
			});
		}
	}
}

void PaimonMultiFileList::DiscoverFilesFromBucketDirs(const string &base_dir, FileSystem &fs) {
	// BFS to find all bucket directories and their data files. Paimon partition nesting is one level
	// per partition key (a handful at most), so an enormous frontier means a pathological or hostile
	// directory tree — cap the work to keep discovery bounded rather than walk forever.
	static constexpr idx_t MAX_DIRS_VISITED = 1u << 20; // ~1M directories
	idx_t dirs_visited = 0;
	vector<string> dirs_to_search;
	dirs_to_search.push_back(base_dir);

	while (!dirs_to_search.empty()) {
		if (++dirs_visited > MAX_DIRS_VISITED) {
			throw IOException("Paimon file discovery exceeded " + std::to_string(MAX_DIRS_VISITED) +
			                  " directories under " + base_dir + " (malformed or unbounded partition tree)");
		}
		string current_dir = dirs_to_search.back();
		dirs_to_search.pop_back();

		try {
			fs.ListFiles(current_dir, [&](const string &name, bool is_dir) {
				if (is_dir) {
					string full_path = current_dir + "/" + name;
					if (StringUtil::StartsWith(name, "bucket-")) {
						// Found a bucket directory — scan for data files
						fs.ListFiles(full_path, [&](const string &fname, bool is_file_dir) {
							if (!is_file_dir &&
							    (StringUtil::EndsWith(fname, ".parquet") || StringUtil::EndsWith(fname, ".orc"))) {
								files.push_back(full_path + "/" + fname);
							}
						});
					} else if (name.find('=') != string::npos) {
						// Looks like a partition directory (partition_key=value)
						dirs_to_search.push_back(full_path);
					}
				}
			});
		} catch (const std::exception &e) {
			// Directory might not exist or be accessible
		}
	}
}

//===--------------------------------------------------------------------===//
// Bind - Set return schema from Paimon metadata
//===--------------------------------------------------------------------===//

void PaimonMultiFileList::Bind(vector<LogicalType> &return_types, vector<string> &names,
                               const PaimonOptions &options) {
	// Try to load metadata if not already loaded
	if (!metadata) {
		try {
			FileSystem &fs = FileSystem::GetFileSystem(context);
			auto paimon_meta_path = PaimonTableMetadata::GetMetaDataPath(context, path, fs, options);
			metadata = PaimonTableMetadata::Parse(paimon_meta_path, fs, options.metadata_compression_codec);
		} catch (const std::exception &e) {
			// Fall through to file-based inference
		}
	}

	// Use schema from metadata if available
	if (metadata && metadata->schema && !metadata->schema->fields.empty()) {
		for (auto &field : metadata->schema->fields) {
			names.push_back(field.name);
			return_types.push_back(PaimonTypeToLogicalType(field.type));
		}
		return;
	}

	// If no metadata schema, let parquet reader infer the schema (return empty and let DuckDB handle it)
	// The parquet_scan MultiFileReader will infer from the first file
}

//===--------------------------------------------------------------------===//
// MultiFileList API
//===--------------------------------------------------------------------===//

vector<OpenFileInfo> PaimonMultiFileList::GetAllFiles() {
	vector<OpenFileInfo> result;
	for (idx_t i = 0; i < files.size(); i++) {
		result.push_back(GetFile(i));
	}
	return result;
}

FileExpandResult PaimonMultiFileList::GetExpandResult() {
	if (files.empty()) {
		return FileExpandResult::NO_FILES;
	}
	if (files.size() == 1) {
		return FileExpandResult::SINGLE_FILE;
	}
	return FileExpandResult::MULTIPLE_FILES;
}

idx_t PaimonMultiFileList::GetTotalFileCount() {
	return files.size();
}

OpenFileInfo PaimonMultiFileList::GetFile(idx_t i) {
	// Contract (see MultiFileList::Scan): return an empty OpenFileInfo once the index runs past
	// the end of the file list, rather than throwing — this is how the scanner detects EOF.
	if (i >= files.size()) {
		return OpenFileInfo();
	}
	return {files[i]};
}

//===--------------------------------------------------------------------===//
// Partition Pruning
//===--------------------------------------------------------------------===//

unique_ptr<MultiFileList>
PaimonMultiFileList::DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
                                           const vector<string> &names, const vector<LogicalType> &types,
                                           const vector<column_t> &column_ids, TableFilterSet &filters) const {
	if (filters.filters.empty()) {
		return nullptr;
	}

	vector<string> filtered_files;

	for (const auto &file : files) {
		bool include_file = true;

		for (const auto &filter_entry : filters.filters) {
			auto column_idx = filter_entry.first;
			if (column_idx >= names.size()) {
				continue;
			}

			const auto &column_name = names[column_idx];
			const auto &filter = *filter_entry.second;

			// Extract partition value from file path for this column
			string partition_value = ExtractPartitionValueFromPath(file, column_name);

			if (!partition_value.empty()) {
				if (!PartitionValueMatchesFilter(partition_value, types[column_idx], filter)) {
					include_file = false;
					break;
				}
			}
		}

		if (include_file) {
			filtered_files.push_back(file);
		}
	}

	if (filtered_files.size() != files.size()) {
		auto filtered_list = make_uniq<PaimonMultiFileList>(context, path, filtered_files);
		if (metadata) {
			// Share metadata with filtered list
			filtered_list->metadata = make_uniq<PaimonTableMetadata>();
			filtered_list->metadata->table_format_version = metadata->table_format_version;
			filtered_list->metadata->table_location = metadata->table_location;
			if (metadata->schema) {
				filtered_list->metadata->schema = make_uniq<PaimonSchema>(*metadata->schema);
			}
			filtered_list->metadata->snapshots = metadata->snapshots;
		}
		return std::move(filtered_list);
	}

	return nullptr;
}

string PaimonMultiFileList::ExtractPartitionValueFromPath(const string &file_path,
                                                          const string &column_name) const {
	string search_pattern = "/" + column_name + "=";
	size_t pos = file_path.find(search_pattern);
	if (pos == string::npos) {
		return "";
	}

	pos += search_pattern.length();
	size_t end_pos = file_path.find("/", pos);
	if (end_pos == string::npos) {
		end_pos = file_path.length();
	}

	return file_path.substr(pos, end_pos - pos);
}

bool PaimonMultiFileList::PartitionValueMatchesFilter(const string &partition_value, const LogicalType &type,
                                                      const TableFilter &filter) const {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		switch (constant_filter.comparison_type) {
		case ExpressionType::COMPARE_EQUAL:
			return partition_value == constant_filter.constant.ToString();
		case ExpressionType::COMPARE_GREATERTHAN:
			return partition_value > constant_filter.constant.ToString();
		case ExpressionType::COMPARE_LESSTHAN:
			return partition_value < constant_filter.constant.ToString();
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			return partition_value >= constant_filter.constant.ToString();
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
			return partition_value <= constant_filter.constant.ToString();
		default:
			return true;
		}
	}
	default:
		return true; // Conservative: include file for unsupported filter types
	}
}

} // namespace duckdb
