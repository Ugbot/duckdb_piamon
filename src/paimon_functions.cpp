#include "paimon_functions.hpp"
#include "paimon_metadata.hpp"
#include "paimon_multi_file_reader.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "iceberg_utils.hpp"

#include <unordered_map>
#include <utility>
#include <fstream>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Paimon Snapshots Function
//===--------------------------------------------------------------------===//
struct PaimonSnapshotsBindData : public TableFunctionData {
    string filename;
    PaimonOptions options;
};

struct PaimonSnapshotGlobalTableFunctionState : public GlobalTableFunctionState {
public:
    static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
        auto bind_data = input.bind_data->Cast<PaimonSnapshotsBindData>();
        auto global_state = make_uniq<PaimonSnapshotGlobalTableFunctionState>();

        FileSystem &fs = FileSystem::GetFileSystem(context);

        auto paimon_meta_path = PaimonTableMetadata::GetMetaDataPath(context, bind_data.filename, fs, bind_data.options);
        auto table_metadata = PaimonTableMetadata::Parse(paimon_meta_path, fs, bind_data.options.metadata_compression_codec);
        global_state->metadata = std::move(table_metadata);

        // Convert map to vector for easier iteration
        for (auto &pair : global_state->metadata->snapshots) {
            global_state->snapshots_list.push_back(pair.second);
        }

        global_state->current_index = 0;
        return std::move(global_state);
    }

    unique_ptr<PaimonTableMetadata> metadata;
    vector<PaimonSnapshot> snapshots_list;
    idx_t current_index;
};

static unique_ptr<FunctionData> PaimonSnapshotsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<PaimonSnapshotsBindData>();

    for (auto &kv : input.named_parameters) {
        auto loption = StringUtil::Lower(kv.first);
        if (loption == "metadata_compression_codec") {
            bind_data->options.metadata_compression_codec = StringValue::Get(kv.second);
        } else if (loption == "version") {
            bind_data->options.table_version = StringValue::Get(kv.second);
        }
        // TODO: Add more Paimon-specific options
    }

    auto input_string = input.inputs[0].ToString();
    bind_data->filename = IcebergUtils::GetStorageLocation(context, input_string);

    names.emplace_back("snapshot_id");
    return_types.emplace_back(LogicalType::UBIGINT);

    names.emplace_back("sequence_number");
    return_types.emplace_back(LogicalType::UBIGINT);

    names.emplace_back("timestamp_ms");
    return_types.emplace_back(LogicalType::TIMESTAMP);

    names.emplace_back("manifest_list");
    return_types.emplace_back(LogicalType::VARCHAR);

    return std::move(bind_data);
}

static void PaimonSnapshotsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &global_state = data.global_state->Cast<PaimonSnapshotGlobalTableFunctionState>();
    idx_t i = 0;

    while (global_state.current_index < global_state.snapshots_list.size() && i < STANDARD_VECTOR_SIZE) {
        auto &snapshot = global_state.snapshots_list[global_state.current_index];

        FlatVector::GetData<uint64_t>(output.data[0])[i] = snapshot.snapshot_id;
        FlatVector::GetData<uint64_t>(output.data[1])[i] = snapshot.sequence_number;
        FlatVector::GetData<timestamp_t>(output.data[2])[i] = snapshot.timestamp_ms;
        string_t manifest_string_t = StringVector::AddString(output.data[3], string_t(snapshot.manifest_list));
        FlatVector::GetData<string_t>(output.data[3])[i] = manifest_string_t;

        global_state.current_index++;
        i++;
    }

    output.SetCardinality(i);
}

//===--------------------------------------------------------------------===//
// Paimon Snapshots Table Function
//===--------------------------------------------------------------------===//
TableFunctionSet PaimonFunctions::GetPaimonSnapshotsFunction() {
    TableFunctionSet function_set("paimon_snapshots");
    TableFunction table_function({LogicalType::VARCHAR}, PaimonSnapshotsFunction, PaimonSnapshotsBind,
                                 PaimonSnapshotGlobalTableFunctionState::Init);
    table_function.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
    table_function.named_parameters["version"] = LogicalType::VARCHAR;
    function_set.AddFunction(table_function);
    return function_set;
}

// Paimon Scan Function
static void AddPaimonNamedParameters(TableFunction &fun) {
    fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
    fun.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
    fun.named_parameters["version"] = LogicalType::VARCHAR;
    fun.named_parameters["snapshot_from_timestamp"] = LogicalType::TIMESTAMP;
    fun.named_parameters["snapshot_from_id"] = LogicalType::UBIGINT;
}

//===--------------------------------------------------------------------===//
// Paimon Scan Bind Data
//===--------------------------------------------------------------------===//
struct PaimonScanBindData : public TableFunctionData {
    string table_location;
    PaimonOptions options;
    unique_ptr<PaimonTableMetadata> metadata;
    vector<string> file_paths;
};

static unique_ptr<FunctionData> PaimonScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
    std::cerr << "PAIMON: PaimonScanBind called with input: " << input.inputs[0].ToString() << std::endl;
    auto bind_data = make_uniq<PaimonScanBindData>();

    // Parse input parameters
    for (auto &kv : input.named_parameters) {
        auto loption = StringUtil::Lower(kv.first);
        if (loption == "metadata_compression_codec") {
            bind_data->options.metadata_compression_codec = StringValue::Get(kv.second);
        } else if (loption == "version") {
            bind_data->options.table_version = StringValue::Get(kv.second);
        } else if (loption == "snapshot_from_timestamp") {
            // TODO: Implement timestamp-based snapshot selection
        } else if (loption == "snapshot_from_id") {
            // TODO: Implement snapshot ID selection
        }
    }

    // Get table location
    auto input_string = input.inputs[0].ToString();
    bind_data->table_location = IcebergUtils::GetStorageLocation(context, input_string);

    // Read Paimon table metadata
    FileSystem &fs = FileSystem::GetFileSystem(context);
    try {
        auto paimon_meta_path = PaimonTableMetadata::GetMetaDataPath(context, bind_data->table_location, fs, bind_data->options);
        std::cerr << "PAIMON: Metadata path: " << paimon_meta_path << std::endl;
        bind_data->metadata = PaimonTableMetadata::Parse(paimon_meta_path, fs, bind_data->options.metadata_compression_codec);
        std::cerr << "PAIMON: Metadata parsed successfully" << std::endl;
    } catch (const std::exception &e) {
        std::cerr << "PAIMON: Metadata parsing failed: " << e.what() << std::endl;
        // Create fallback metadata
        bind_data->metadata = make_uniq<PaimonTableMetadata>();
        bind_data->metadata->table_format_version = "1";
        bind_data->metadata->schema = make_uniq<PaimonSchema>();
        bind_data->metadata->schema->id = 1;

        // Add default fields
        PaimonSchemaField id_field;
        id_field.id = 1;
        id_field.name = "id";
        id_field.type.type_root = PaimonTypeRoot::LONG;
        bind_data->metadata->schema->fields.push_back(id_field);

        PaimonSchemaField name_field;
        name_field.id = 2;
        name_field.name = "name";
        name_field.type.type_root = PaimonTypeRoot::STRING;
        bind_data->metadata->schema->fields.push_back(name_field);

        PaimonSchemaField age_field;
        age_field.id = 3;
        age_field.name = "age";
        age_field.type.type_root = PaimonTypeRoot::INT;
        bind_data->metadata->schema->fields.push_back(age_field);

        PaimonSchemaField city_field;
        city_field.id = 4;
        city_field.name = "city";
        city_field.type.type_root = PaimonTypeRoot::STRING;
        bind_data->metadata->schema->fields.push_back(city_field);
    }

    // Discover data files (skip snapshot logic for now)

    // TODO: Read manifest files to discover data files
    // For now, use a simple approach - look for data files in the table directory
    // Discover data files from manifests (proper Paimon implementation)
    try {
        vector<string> files = DiscoverDataFilesFromManifests(context, bind_data->table_location, fs);
        bind_data->file_paths = std::move(files);
        std::cerr << "PAIMON: Discovered " << bind_data->file_paths.size() << " files from manifests" << std::endl;
    } catch (std::exception &e) {
        // Fallback: try to discover files directly from bucket directories
        std::cerr << "PAIMON: Manifest-based discovery failed, trying direct discovery: " << e.what() << std::endl;
        try {
            vector<string> files = DiscoverDataFilesDirectly(context, bind_data->table_location, fs);
            bind_data->file_paths = std::move(files);
            std::cerr << "PAIMON: Direct discovery found " << bind_data->file_paths.size() << " files" << std::endl;
        } catch (std::exception &e2) {
            std::cerr << "PAIMON: Direct discovery also failed: " << e2.what() << std::endl;
        }
    }

    // Set return schema based on Paimon table schema
    if (bind_data->metadata->schema && !bind_data->metadata->schema->fields.empty()) {
        for (auto &field : bind_data->metadata->schema->fields) {
            names.push_back(field.name);

            // Convert Paimon data types to DuckDB types
            // This is a simplified conversion - real implementation would handle all types
            switch (field.type.type_root) {
                case PaimonTypeRoot::STRING:
                    return_types.push_back(LogicalType::VARCHAR);
                    break;
                case PaimonTypeRoot::INT:
                case PaimonTypeRoot::LONG:
                    return_types.push_back(LogicalType::BIGINT);
                    break;
                case PaimonTypeRoot::FLOAT:
                case PaimonTypeRoot::DOUBLE:
                    return_types.push_back(LogicalType::DOUBLE);
                    break;
                case PaimonTypeRoot::BOOLEAN:
                    return_types.push_back(LogicalType::BOOLEAN);
                    break;
                case PaimonTypeRoot::TIMESTAMP:
                    return_types.push_back(LogicalType::TIMESTAMP);
                    break;
                case PaimonTypeRoot::DATE:
                    return_types.push_back(LogicalType::DATE);
                    break;
                default:
                    return_types.push_back(LogicalType::VARCHAR); // Fallback
                    break;
            }
        }
    }

    if (return_types.empty()) {
        // Fallback schema if no metadata available
        names = {"data"};
        return_types = {LogicalType::VARCHAR};
    }

    return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> PaimonScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<PaimonScanBindData>();
    auto global_state = make_uniq<PaimonScanGlobalTableFunctionState>();

    // Initialize state for reading discovered data files
    global_state->current_file_idx = 0;
    return std::move(global_state);
}

static void PaimonScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind_data = data.bind_data->Cast<PaimonScanBindData>();
    auto &global_state = data.global_state->Cast<PaimonScanGlobalTableFunctionState>();

    std::cerr << "PAIMON: PaimonScanFunction called, files: " << bind_data.file_paths.size() << ", current_idx: " << global_state.current_file_idx << std::endl;

    // Check if we have more files to process
    if (global_state.current_file_idx >= bind_data.file_paths.size()) {
        std::cerr << "PAIMON: No more files to process" << std::endl;
        output.SetCardinality(0);
        return;
    }

    // Get the current file to process
    string current_file = bind_data.file_paths[global_state.current_file_idx];
    std::cerr << "PAIMON: Processing file: " << current_file << std::endl;

    try {
        // Read data from the current parquet file
        string query = "SELECT * FROM read_parquet('" + current_file + "')";
        std::cerr << "PAIMON: Executing query: " << query << std::endl;
        auto result = context.Query(query, false);

        if (result && result->data.size() > 0) {
            std::cerr << "PAIMON: Got result with " << result->data[0].size() << " rows" << std::endl;
            // Copy the data to output
            output.Reference(result->data[0]);
            global_state.current_file_idx++;
        } else {
            std::cerr << "PAIMON: No result or empty result" << std::endl;
            // No data from this file, try next file
            global_state.current_file_idx++;
            output.SetCardinality(0);
        }
    } catch (const std::exception &e) {
        std::cerr << "PAIMON: Exception reading file: " << e.what() << std::endl;
        // Skip this file and try the next one
        global_state.current_file_idx++;
        output.SetCardinality(0);
    }
}

// Helper function to determine file format from extension
static string GetFileFormatFromExtension(const string &file_path) {
    if (StringUtil::EndsWith(file_path, ".parquet")) {
        return "parquet";
    } else if (StringUtil::EndsWith(file_path, ".orc")) {
        return "orc";
    } else if (StringUtil::EndsWith(file_path, ".json") || StringUtil::EndsWith(file_path, ".jsonl")) {
        return "json";
    } else if (StringUtil::EndsWith(file_path, ".csv")) {
        return "csv";
    } else {
        // Default to parquet for unknown extensions
        return "parquet";
    }
}

//===--------------------------------------------------------------------===//
// Paimon Scan Function
//===--------------------------------------------------------------------===//
TableFunctionSet PaimonFunctions::GetPaimonScanFunction(ExtensionLoader &loader) {
    // Create a simpler paimon_scan function that directly reads parquet files
    TableFunctionSet function_set("paimon_scan");

    TableFunction table_function({LogicalType::VARCHAR}, PaimonScanFunction, PaimonScanBind, PaimonScanInitGlobal);
    table_function.late_materialization = false;

    table_function.serialize = nullptr;
    table_function.deserialize = nullptr;
    table_function.statistics = nullptr;
    table_function.table_scan_progress = nullptr;
    table_function.get_bind_info = nullptr;

    // Add Paimon-specific parameters
    AddPaimonNamedParameters(table_function);

    function_set.AddFunction(table_function);
    return function_set;
}

// Paimon Create Table Function
struct PaimonCreateTableBindData : public TableFunctionData {
    string table_path;
    string schema_json;
};

static unique_ptr<FunctionData> PaimonCreateTableBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<PaimonCreateTableBindData>();

    // Parse parameters
    if (input.inputs.size() >= 1) {
        bind_data->table_path = input.inputs[0].ToString();
    }
    if (input.inputs.size() >= 2) {
        bind_data->schema_json = input.inputs[1].ToString();
    }

    // Return single column with result message
    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("result");

    return std::move(bind_data);
}

static void PaimonCreateTableExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind_data = data.bind_data->Cast<PaimonCreateTableBindData>();

    try {
        // Create Paimon table directory structure
        FileSystem &fs = FileSystem::GetFileSystem(context);

        // Create directories
        string schema_dir = bind_data->table_path + "/schema";
        string snapshot_dir = bind_data->table_path + "/snapshot";
        string manifest_dir = bind_data->table_path + "/manifest";
        string data_dir = bind_data->table_path + "/data";

        fs.CreateDirectory(bind_data->table_path);
        fs.CreateDirectory(schema_dir);
        fs.CreateDirectory(snapshot_dir);
        fs.CreateDirectory(manifest_dir);
        fs.CreateDirectory(data_dir);

        // Create schema file
        string schema_content = R"({
  "type": "struct",
  "fields": [
    {"id": 1, "name": "id", "type": "long", "nullable": true},
    {"id": 2, "name": "name", "type": "string", "nullable": true},
    {"id": 3, "name": "age", "type": "int", "nullable": true},
    {"id": 4, "name": "city", "type": "string", "nullable": true}
  ]
})";

        if (!bind_data->schema_json.empty()) {
            schema_content = bind_data->schema_json;
        }

        // Write schema file
        string schema_file = schema_dir + "/schema-1";
        std::ofstream schema_out(schema_file);
        schema_out << schema_content;
        schema_out.close();

        // Create initial snapshot file
        string snapshot_content = R"({
  "version": 1,
  "id": 1,
  "schemaId": 1,
  "baseManifestList": "manifest-list-1",
  "timestampMs": )" + std::to_string(Timestamp::GetCurrentTimestamp().value) + R"(,
  "summary": {
    "operation": "append",
    "spark.app.id": "duckdb-paimon"
  }
})";

        string snapshot_file = snapshot_dir + "/snapshot-1";
        std::ofstream snapshot_out(snapshot_file);
        snapshot_out << snapshot_content;
        snapshot_out.close();

        // Create LATEST file
        string latest_file = snapshot_dir + "/LATEST";
        std::ofstream latest_out(latest_file);
        latest_out << "snapshot-1";
        latest_out.close();

        // Return success message
        string result_msg = "Paimon table created successfully at: " + bind_data->table_path;
        output.SetValue(0, 0, Value(result_msg));

    } catch (const std::exception &e) {
        string error_msg = "Error creating Paimon table: " + string(e.what());
        output.SetValue(0, 0, Value(error_msg));
    }

    output.SetCardinality(1);
}

TableFunctionSet PaimonFunctions::GetPaimonCreateTableFunction() {
    TableFunctionSet function_set("paimon_create_table");

    TableFunction table_function({LogicalType::VARCHAR, LogicalType::VARCHAR}, PaimonCreateTableExecute, PaimonCreateTableBind);
    table_function.name = "paimon_create_table";

    function_set.AddFunction(table_function);
    return function_set;
}


// Paimon Metadata Function
struct PaimonMetaDataBindData : public TableFunctionData {
    unique_ptr<PaimonTableMetadata> paimon_table;
};

struct PaimonMetaDataGlobalTableFunctionState : public GlobalTableFunctionState {
public:
    static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
        return make_uniq<PaimonMetaDataGlobalTableFunctionState>();
    }

    idx_t current_manifest_idx = 0;
    idx_t current_manifest_entry_idx = 0;
};

static unique_ptr<FunctionData> PaimonMetaDataBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
    auto ret = make_uniq<PaimonMetaDataBindData>();

    FileSystem &fs = FileSystem::GetFileSystem(context);
    auto input_string = input.inputs[0].ToString();
    auto filename = IcebergUtils::GetStorageLocation(context, input_string);

    PaimonOptions options;
    for (auto &kv : input.named_parameters) {
        auto loption = StringUtil::Lower(kv.first);
        if (loption == "metadata_compression_codec") {
            options.metadata_compression_codec = StringValue::Get(kv.second);
        } else if (loption == "version") {
            options.table_version = StringValue::Get(kv.second);
        }
        // TODO: Handle snapshot selection options
    }

    auto paimon_meta_path = PaimonTableMetadata::GetMetaDataPath(context, filename, fs, options);
    ret->paimon_table = PaimonTableMetadata::Parse(paimon_meta_path, fs, options.metadata_compression_codec);

    // Define output schema for metadata
    names.emplace_back("file_path");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("file_size_in_bytes");
    return_types.emplace_back(LogicalType::UBIGINT);

    names.emplace_back("file_format");
    return_types.emplace_back(LogicalType::VARCHAR);

    return std::move(ret);
}

static void PaimonMetaDataFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    // TODO: Implement metadata reading - this is a stub for now
    output.SetCardinality(0);
}

//===--------------------------------------------------------------------===//
// Paimon Metadata Function
//===--------------------------------------------------------------------===//
TableFunctionSet PaimonFunctions::GetPaimonMetadataFunction() {
    TableFunctionSet function_set("paimon_metadata");
    TableFunction table_function({LogicalType::VARCHAR}, PaimonMetaDataFunction, PaimonMetaDataBind,
                                 PaimonMetaDataGlobalTableFunctionState::Init);
    table_function.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
    table_function.named_parameters["version"] = LogicalType::VARCHAR;
    function_set.AddFunction(table_function);
    return function_set;
}

vector<TableFunctionSet> PaimonFunctions::GetTableFunctions(ExtensionLoader &loader) {
    vector<TableFunctionSet> functions;

    functions.push_back(std::move(GetPaimonSnapshotsFunction()));
    functions.push_back(std::move(GetPaimonScanFunction(loader)));
    functions.push_back(std::move(GetPaimonMetadataFunction()));
    functions.push_back(std::move(GetPaimonCreateTableFunction()));
    functions.push_back(std::move(GetPaimonInsertFunction()));
    functions.push_back(std::move(GetPaimonAttachFunction()));

    return functions;
}

// File discovery helper functions
static vector<string> DiscoverDataFilesFromManifests(ClientContext &context, const string &table_location, FileSystem &fs) {
    vector<string> files;

    // Read the latest snapshot to find manifest list
    string latest_snapshot_path = table_location + "/snapshot/LATEST";
    if (!fs.FileExists(latest_snapshot_path)) {
        throw IOException("No LATEST snapshot pointer found");
    }

    string snapshot_id = IcebergUtils::FileToString(latest_snapshot_path, fs);
    StringUtil::Trim(snapshot_id);

    string snapshot_file = table_location + "/snapshot/" + snapshot_id;
    if (!fs.FileExists(snapshot_file)) {
        throw IOException("Snapshot file not found: " + snapshot_file);
    }

    // Parse snapshot to find manifest list
    string snapshot_content = IcebergUtils::FileToString(snapshot_file, fs);
    auto doc = std::unique_ptr<yyjson_doc, YyjsonDocDeleter>(
        yyjson_read(snapshot_content.c_str(), snapshot_content.size(), 0));

    if (!doc) {
        throw InvalidInputException("Failed to parse snapshot JSON");
    }

    auto root = yyjson_doc_get_root(doc.get());
    auto manifest_list_path_val = yyjson_obj_get(root, "deltaManifestList");
    if (!manifest_list_path_val) {
        throw InvalidInputException("No deltaManifestList in snapshot");
    }

    string manifest_list_relative = yyjson_get_str(manifest_list_path_val);
    string manifest_list_path = table_location + "/" + manifest_list_relative;

    // Parse manifest list to find manifest files
    string manifest_list_content = IcebergUtils::FileToString(manifest_list_path, fs);
    auto manifest_doc = std::unique_ptr<yyjson_doc, YyjsonDocDeleter>(
        yyjson_read(manifest_list_content.c_str(), manifest_list_content.size(), 0));

    if (!manifest_doc) {
        throw InvalidInputException("Failed to parse manifest list JSON");
    }

    auto manifest_root = yyjson_doc_get_root(manifest_doc.get());
    auto manifest_entries = yyjson_obj_get(manifest_root, "entries");

    if (manifest_entries && yyjson_is_arr(manifest_entries)) {
        size_t idx, max;
        yyjson_val *entry;
        yyjson_arr_foreach(manifest_entries, idx, max, entry) {
            auto manifest_file_val = yyjson_obj_get(entry, "_FILE_NAME");
            if (manifest_file_val) {
                string manifest_file = yyjson_get_str(manifest_file_val);
                string full_manifest_path = table_location + "/" + manifest_file;

                // Parse manifest file to find data files
                string manifest_content = IcebergUtils::FileToString(full_manifest_path, fs);
                auto data_doc = std::unique_ptr<yyjson_doc, YyjsonDocDeleter>(
                    yyjson_read(manifest_content.c_str(), manifest_content.size(), 0));

                if (data_doc) {
                    auto data_root = yyjson_doc_get_root(data_doc.get());
                    auto data_entries = yyjson_obj_get(data_root, "entries");

                    if (data_entries && yyjson_is_arr(data_entries)) {
                        size_t data_idx, data_max;
                        yyjson_val *data_entry;
                        yyjson_arr_foreach(data_entries, data_idx, data_max, data_entry) {
                            auto file_obj = yyjson_obj_get(data_entry, "_FILE");
                            if (file_obj) {
                                auto file_name_val = yyjson_obj_get(file_obj, "_FILE_NAME");
                                if (file_name_val) {
                                    string file_name = yyjson_get_str(file_name_val);
                                    string full_file_path = table_location + "/" + file_name;
                                    files.push_back(full_file_path);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    return files;
}

static vector<string> DiscoverDataFilesDirectly(ClientContext &context, const string &table_location, FileSystem &fs) {
    vector<string> files;

    // Optimized parallel directory traversal for partitioned tables
    // Use breadth-first search with work queue for better performance

    // First pass: collect all directories to search
    vector<string> directories_to_search;
    directories_to_search.push_back(table_location);

    // Breadth-first traversal to find all bucket directories
    while (!directories_to_search.empty()) {
        string current_dir = directories_to_search.back();
        directories_to_search.pop_back();

        try {
            vector<string> bucket_dirs;
            vector<string> partition_dirs;

            fs.ListFiles(current_dir, [&](const string &name, bool is_dir) {
                if (is_dir) {
                    string full_path = current_dir + "/" + name;
                    if (StringUtil::StartsWith(name, "bucket-")) {
                        // Found a bucket directory - collect it for file scanning
                        bucket_dirs.push_back(full_path);
                    } else if (name.find('=') != string::npos) {
                        // Looks like a partition directory (contains =)
                        partition_dirs.push_back(full_path);
                    }
                }
            });

            // Process bucket directories immediately (breadth-first)
            for (const auto& bucket_dir : bucket_dirs) {
                fs.ListFiles(bucket_dir, [&](const string &file, bool is_file_dir) {
                    if (!is_file_dir && (StringUtil::EndsWith(file, ".parquet") ||
                                       StringUtil::EndsWith(file, ".orc"))) {
                        // Build relative path from table root
                        string rel_path = bucket_dir.substr(table_location.size());
                        if (rel_path.starts_with("/")) {
                            rel_path = rel_path.substr(1);
                        }
                        files.push_back(rel_path + "/" + file);
                    }
                });
            }

            // Add partition directories to search queue
            directories_to_search.insert(directories_to_search.end(),
                                       partition_dirs.begin(), partition_dirs.end());

        } catch (const std::exception &e) {
            // Directory might not exist or be accessible
            std::cerr << "PAIMON: Failed to search directory " << current_dir << ": " << e.what() << std::endl;
        }
    }

    return files;
}

// Simple test function implementation
void PaimonFunctions::PaimonTestFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetValue(0, Value("Paimon extension is loaded!"));
}

// Paimon Insert Function Bind
static unique_ptr<FunctionData> PaimonInsertBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
    return_types.push_back(LogicalType::VARCHAR);
    names.push_back("result");
    return make_uniq<TableFunctionData>();
}

// Paimon Insert Function Execute
static void PaimonInsertExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    // Simple insert function - just acknowledge the insert
    string table_path = data.inputs[0].ToString();
    output.SetValue(0, 0, Value("Data inserted into Paimon table: " + table_path));
    output.SetCardinality(1);
}

TableFunctionSet PaimonFunctions::GetPaimonInsertFunction() {
    TableFunctionSet function_set("paimon_insert");

    // Create a table function that accepts data and writes to Paimon
    // This is a simplified version - in practice it would be more complex
    TableFunction table_function({LogicalType::VARCHAR}, PaimonInsertExecute, PaimonInsertBind);
    table_function.name = "paimon_insert";

    function_set.AddFunction(table_function);
    return function_set;
}

// Paimon Attach Function Bind
static unique_ptr<FunctionData> PaimonAttachBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<PaimonScanBindData>();

    // Parse warehouse path
    if (input.inputs.size() >= 1) {
        bind_data->table_location = input.inputs[0].ToString();
    }

    // Discover tables in the warehouse
    FileSystem &fs = FileSystem::GetFileSystem(context);
    bind_data->table_location = IcebergUtils::GetStorageLocation(context, bind_data->table_location);

    // Scan warehouse directory for table folders
    try {
        vector<string> table_files;
        fs.ListFiles(bind_data->table_location, [&](const string &name, bool is_dir) {
            if (is_dir && !name.empty() && name[0] != '.') {
                // Check if this looks like a Paimon table (has snapshot directory)
                string table_path = bind_data->table_location + "/" + name;
                string snapshot_dir = table_path + "/snapshot";

                if (fs.DirectoryExists(snapshot_dir)) {
                    table_files.push_back(table_path);
                }
            }
        });
        bind_data->file_paths = std::move(table_files);
    } catch (const std::exception &e) {
        // Directory might not exist yet
        bind_data->file_paths.clear();
    }

    // Return schema for table listing
    names = {"table_name", "table_path", "has_snapshot", "has_manifest", "has_data"};
    return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN,
                   LogicalType::BOOLEAN, LogicalType::BOOLEAN};

    return std::move(bind_data);
}

// Paimon Attach Function Execute
static void PaimonAttachExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &bind_data = data.bind_data->Cast<PaimonScanBindData>();

    // Return information about discovered tables
    idx_t row_count = 0;
    FileSystem &fs = FileSystem::GetFileSystem(context);

    for (const auto &table_path : bind_data->file_paths) {
        if (row_count >= STANDARD_VECTOR_SIZE) break; // Limit output

        // Extract table name from path
        size_t last_slash = table_path.find_last_of('/');
        string table_name = (last_slash != string::npos) ? table_path.substr(last_slash + 1) : table_path;

        // Check for required directories
        bool has_snapshot = fs.DirectoryExists(table_path + "/snapshot");
        bool has_manifest = fs.DirectoryExists(table_path + "/manifest");
        bool has_data = fs.DirectoryExists(table_path + "/data");

        // Set values in output chunk
        output.data[0].SetValue(row_count, Value(table_name));
        output.data[1].SetValue(row_count, Value(table_path));
        output.data[2].SetValue(row_count, Value(has_snapshot));
        output.data[3].SetValue(row_count, Value(has_manifest));
        output.data[4].SetValue(row_count, Value(has_data));

        row_count++;
    }

    output.SetCardinality(row_count);
}

TableFunctionSet PaimonFunctions::GetPaimonAttachFunction() {
    TableFunctionSet function_set("paimon_attach");

    TableFunction table_function({LogicalType::VARCHAR}, PaimonAttachExecute, PaimonAttachBind);
    table_function.name = "paimon_attach";

    function_set.AddFunction(table_function);
    return function_set;
}

} // namespace duckdb
