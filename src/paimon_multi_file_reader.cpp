#include "paimon_multi_file_reader.hpp"
#include "paimon_multi_file_list.hpp"
#include "paimon_predicate.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/multi_file_options.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "iceberg_utils.hpp"

namespace duckdb {

PaimonMultiFileReader::PaimonMultiFileReader(shared_ptr<TableFunctionInfo> function_info)
    : MultiFileReader(), function_info(std::move(function_info)) {
}

unique_ptr<MultiFileReader> PaimonMultiFileReader::CreateInstance(const TableFunction &table) {
    return make_uniq<PaimonMultiFileReader>(table.function_info);
}

shared_ptr<MultiFileList> PaimonMultiFileReader::CreateFileList(ClientContext &context, const vector<string> &paths,
                                                                const FileGlobInput &glob_input) {
    // Parse Paimon table location and discover files
    string table_location = paths.empty() ? "" : paths[0];
    std::cerr << "PAIMON: CreateFileList called with table_location: " << table_location << std::endl;
    vector<string> discovered_files = DiscoverPaimonFiles(context, table_location);

    std::cerr << "PAIMON: CreateFileList found " << discovered_files.size() << " files" << std::endl;

    return make_shared_ptr<PaimonMultiFileList>(context, table_location, discovered_files);
}

bool PaimonMultiFileReader::Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                                 vector<string> &names, MultiFileReaderBindData &bind_data) {
    std::cerr << "PAIMON: MultiFileReader::Bind called" << std::endl;
    auto &paimon_multi_file_list = dynamic_cast<PaimonMultiFileList &>(files);

    // Bind the schema from Paimon metadata using the correct options
    paimon_multi_file_list.Bind(return_types, names, paimon_options);
    std::cerr << "PAIMON: Bind set " << return_types.size() << " return types and " << names.size() << " names" << std::endl;

    // Determine the file format from the files
    string format = "parquet"; // default
    if (!paimon_multi_file_list.files.empty()) {
        string first_file = paimon_multi_file_list.files[0];
        format = GetFileFormatFromPath(first_file);
    }

    // Store the format for later use
    this->file_format = format;

    // Set up the column mapping based on format
    auto &schema = paimon_multi_file_list.metadata;
    if (schema && schema->schema && !schema->schema->fields.empty()) {
        auto &columns = bind_data.schema;
        for (auto &field : schema->schema->fields) {
            MultiFileColumnDefinition col_def;
            col_def.name = field.name;
            col_def.type = return_types[columns.size()]; // Use the type we just set
            columns.push_back(col_def);
        }
        bind_data.mapping = MultiFileColumnMappingMode::BY_NAME;
    }

    // Parse Paimon-specific options
    BindOptions(options, files, return_types, names, bind_data);

    // Call base class Bind to set up file readers
    return MultiFileReader::Bind(options, files, return_types, names, bind_data);
}

string PaimonMultiFileReader::GetFileFormatFromPath(const string &file_path) const {
    if (StringUtil::EndsWith(file_path, ".parquet")) {
        return "parquet";
    } else if (StringUtil::EndsWith(file_path, ".orc")) {
        return "orc";
    } else if (StringUtil::EndsWith(file_path, ".json") || StringUtil::EndsWith(file_path, ".jsonl")) {
        return "json";
    } else if (StringUtil::EndsWith(file_path, ".csv")) {
        return "csv";
    } else if (StringUtil::EndsWith(file_path, ".arrow")) {
        return "arrow";
    } else {
        // Default to parquet for unknown extensions
        return "parquet";
    }
}

void PaimonMultiFileReader::BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                                        vector<string> &names, MultiFileReaderBindData &bind_data) {
    // Handle Paimon-specific options
    // TODO: Implement option parsing
}

unique_ptr<MultiFileReaderGlobalState>
PaimonMultiFileReader::InitializeGlobalState(ClientContext &context, const MultiFileOptions &file_options,
                                             const MultiFileReaderBindData &bind_data, const MultiFileList &file_list,
                                             const vector<MultiFileColumnDefinition> &global_columns,
                                             const vector<ColumnIndex> &global_column_ids) {
    vector<LogicalType> extra_columns;
    return make_uniq<PaimonMultiFileReaderGlobalState>(std::move(extra_columns), file_list);
}

void PaimonMultiFileReader::FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
                                         const MultiFileReaderBindData &options, const vector<MultiFileColumnDefinition> &global_columns,
                                         const vector<ColumnIndex> &global_column_ids, ClientContext &context,
                                         optional_ptr<MultiFileReaderGlobalState> global_state) {
    // TODO: Implement final bind logic
}

void PaimonMultiFileReader::FinalizeChunk(ClientContext &context, const MultiFileBindData &bind_data, BaseFileReader &reader,
                                          const MultiFileReaderData &reader_data, DataChunk &input_chunk, DataChunk &output_chunk,
                                          ExpressionExecutor &executor, optional_ptr<MultiFileReaderGlobalState> global_state) {
    // Copy input to output (no additional processing for now)
    output_chunk.Reference(input_chunk);
}

bool PaimonMultiFileReader::ParseOption(const string &key, const Value &val, MultiFileOptions &options, ClientContext &context) {
    auto loption = StringUtil::Lower(key);

    // Handle Paimon-specific options
    if (loption == "metadata_compression_codec") {
        // TODO: Store in PaimonOptions
        return true;
    } else if (loption == "version") {
        // TODO: Store in PaimonOptions
        return true;
    } else if (loption == "version_name_format") {
        // TODO: Store in PaimonOptions
        return true;
    } else if (loption == "snapshot_from_timestamp") {
        if (options.snapshot_lookup.snapshot_source != PaimonOptions::SnapshotLookup::SnapshotSource::LATEST) {
            throw InvalidInputException("Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
        }
        options.snapshot_lookup.snapshot_source = PaimonOptions::SnapshotLookup::SnapshotSource::FROM_TIMESTAMP;
        options.snapshot_lookup.snapshot_timestamp = val.GetValue<timestamp_t>();
        return true;
    } else if (loption == "snapshot_from_id") {
        if (options.snapshot_lookup.snapshot_source != PaimonOptions::SnapshotLookup::SnapshotSource::LATEST) {
            throw InvalidInputException("Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
        }
        options.snapshot_lookup.snapshot_source = PaimonOptions::SnapshotLookup::SnapshotSource::FROM_ID;
        options.snapshot_lookup.snapshot_id = val.GetValue<uint64_t>();
        return true;
    }

    // Not a Paimon-specific option
    return false;
}

// Discover Paimon files from table location
vector<string> PaimonMultiFileReader::DiscoverPaimonFiles(ClientContext &context, const string &table_location) {
    vector<string> discovered_files;
    FileSystem &fs = FileSystem::GetFileSystem(context);

    // Look for data files in the data directory
    string data_path = table_location + "/data";
    try {
        fs.ListFiles(data_path, [&](const string &file, bool is_directory) {
            if (!is_directory && (StringUtil::EndsWith(file, ".parquet") ||
                                 StringUtil::EndsWith(file, ".orc") ||
                                 StringUtil::EndsWith(file, ".json"))) {
                string full_path = data_path + "/" + file;
                discovered_files.push_back(full_path);
                // Debug output
                std::cerr << "PAIMON: Found file: " << full_path << std::endl;
            }
        });
    } catch (std::exception &e) {
        // Directory might not exist or be accessible
        std::cerr << "PAIMON: Error listing files in " << data_path << ": " << e.what() << std::endl;
    }

    std::cerr << "PAIMON: Discovered " << discovered_files.size() << " files" << std::endl;
    return discovered_files;
}

// Apply partition pruning to discovered files (placeholder for now)
vector<string> PaimonMultiFileReader::ApplyPartitionPruning(const vector<string> &files,
                                                             const vector<unique_ptr<TableFilter>> &filters) {
    // TODO: Implement partition pruning logic
    // For now, return all files
    return files;
}

} // namespace duckdb
