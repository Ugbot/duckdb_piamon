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

namespace duckdb {

//===--------------------------------------------------------------------===//
// Paimon Named Parameters (shared across functions)
//===--------------------------------------------------------------------===//

static void AddPaimonNamedParameters(TableFunction &fun) {
	fun.named_parameters["allow_moved_paths"] = LogicalType::BOOLEAN;
	fun.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	fun.named_parameters["version"] = LogicalType::VARCHAR;
	fun.named_parameters["snapshot_from_timestamp"] = LogicalType::TIMESTAMP;
	fun.named_parameters["snapshot_from_id"] = LogicalType::UBIGINT;
}

//===--------------------------------------------------------------------===//
// Paimon Scan Function
// Follows the Iceberg pattern: clone parquet_scan and inject PaimonMultiFileReader
//===--------------------------------------------------------------------===//

static void PaimonScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                const TableFunction &function) {
	throw NotImplementedException("PaimonScan serialization not implemented");
}

TableFunctionSet PaimonFunctions::GetPaimonScanFunction(ExtensionLoader &loader) {
	// Clone the parquet_scan function and inject our MultiFileReader
	auto &parquet_scan = loader.GetTableFunction("parquet_scan");
	auto parquet_scan_copy = parquet_scan.functions;

	for (auto &function : parquet_scan_copy.functions) {
		// Inject PaimonMultiFileReader as the driver for reads
		function.get_multi_file_reader = PaimonMultiFileReader::CreateInstance;
		function.late_materialization = false;

		// Disable broken/inefficient features for now
		function.serialize = PaimonScanSerialize;
		function.deserialize = nullptr;
		function.statistics = nullptr;
		function.table_scan_progress = nullptr;
		function.get_bind_info = nullptr;

		// Remove schema param (confusing in this context)
		function.named_parameters.erase("schema");
		AddPaimonNamedParameters(function);

		function.name = "paimon_scan";
	}

	parquet_scan_copy.name = "paimon_scan";
	return parquet_scan_copy;
}

//===--------------------------------------------------------------------===//
// Paimon Snapshots Function
//===--------------------------------------------------------------------===//

struct PaimonSnapshotsBindData : public TableFunctionData {
	string table_location;
	PaimonOptions options;
};

struct PaimonSnapshotGlobalState : public GlobalTableFunctionState {
public:
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		auto &bind_data = input.bind_data->Cast<PaimonSnapshotsBindData>();
		auto global_state = make_uniq<PaimonSnapshotGlobalState>();

		FileSystem &fs = FileSystem::GetFileSystem(context);

		// Discover all snapshot files
		string snapshot_dir = bind_data.table_location + "/snapshot";
		if (fs.DirectoryExists(snapshot_dir)) {
			vector<string> snapshot_files;
			fs.ListFiles(snapshot_dir, [&](const string &fname, bool is_dir) {
				if (!is_dir && fname.find("snapshot-") == 0) {
					snapshot_files.push_back(fname);
				}
			});

			sort(snapshot_files.begin(), snapshot_files.end());

			for (auto &fname : snapshot_files) {
				string full_path = snapshot_dir + "/" + fname;
				try {
					auto meta = PaimonTableMetadata::ParseSnapshotMetadata(full_path, fs,
					                                                      bind_data.options.metadata_compression_codec);
					// Parse full snapshot for all fields
					string json_content = IcebergUtils::FileToString(full_path, fs);
					auto doc = std::unique_ptr<yyjson_doc, YyjsonDocDeleter>(
					    yyjson_read(json_content.c_str(), json_content.size(), 0));
					if (doc) {
						auto root = yyjson_doc_get_root(doc.get());
						PaimonSnapshot snapshot;
						snapshot.snapshot_id = meta.snapshot_id;
						snapshot.timestamp_ms = meta.timestamp_ms;

						auto schema_id = yyjson_obj_get(root, "schemaId");
						if (schema_id && yyjson_is_int(schema_id)) {
							snapshot.schema_id = yyjson_get_int(schema_id);
						}

						auto base_ml = yyjson_obj_get(root, "baseManifestList");
						if (base_ml && yyjson_is_str(base_ml)) {
							snapshot.base_manifest_list = yyjson_get_str(base_ml);
						}

						auto delta_ml = yyjson_obj_get(root, "deltaManifestList");
						if (delta_ml && yyjson_is_str(delta_ml)) {
							snapshot.delta_manifest_list = yyjson_get_str(delta_ml);
						}

						auto commit_kind = yyjson_obj_get(root, "commitKind");
						if (commit_kind && yyjson_is_str(commit_kind)) {
							snapshot.commit_kind = yyjson_get_str(commit_kind);
						}

						// Set legacy manifest_list
						if (!snapshot.delta_manifest_list.empty()) {
							snapshot.manifest_list = snapshot.delta_manifest_list;
						} else if (!snapshot.base_manifest_list.empty()) {
							snapshot.manifest_list = snapshot.base_manifest_list;
						}

						snapshot.sequence_number = snapshot.snapshot_id;
						global_state->snapshots_list.push_back(std::move(snapshot));
					}
				} catch (const std::exception &e) {
					// Skip malformed snapshot files
				}
			}
		}

		global_state->current_index = 0;
		return std::move(global_state);
	}

	vector<PaimonSnapshot> snapshots_list;
	idx_t current_index = 0;
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
	}

	bind_data->table_location = IcebergUtils::GetStorageLocation(context, input.inputs[0].ToString());

	names.emplace_back("snapshot_id");
	return_types.emplace_back(LogicalType::UBIGINT);

	names.emplace_back("schema_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("commit_kind");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("timestamp_ms");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("base_manifest_list");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("delta_manifest_list");
	return_types.emplace_back(LogicalType::VARCHAR);

	return std::move(bind_data);
}

static void PaimonSnapshotsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<PaimonSnapshotGlobalState>();
	idx_t i = 0;

	while (global_state.current_index < global_state.snapshots_list.size() && i < STANDARD_VECTOR_SIZE) {
		auto &snapshot = global_state.snapshots_list[global_state.current_index];

		FlatVector::GetData<uint64_t>(output.data[0])[i] = snapshot.snapshot_id;
		FlatVector::GetData<int64_t>(output.data[1])[i] = snapshot.schema_id;
		FlatVector::GetData<string_t>(output.data[2])[i] = StringVector::AddString(output.data[2], snapshot.commit_kind);
		FlatVector::GetData<int64_t>(output.data[3])[i] = snapshot.timestamp_ms;
		FlatVector::GetData<string_t>(output.data[4])[i] = StringVector::AddString(output.data[4], snapshot.base_manifest_list);
		FlatVector::GetData<string_t>(output.data[5])[i] = StringVector::AddString(output.data[5], snapshot.delta_manifest_list);

		global_state.current_index++;
		i++;
	}

	output.SetCardinality(i);
}

TableFunctionSet PaimonFunctions::GetPaimonSnapshotsFunction() {
	TableFunctionSet function_set("paimon_snapshots");
	TableFunction table_function({LogicalType::VARCHAR}, PaimonSnapshotsFunction, PaimonSnapshotsBind,
	                             PaimonSnapshotGlobalState::Init);
	table_function.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	table_function.named_parameters["version"] = LogicalType::VARCHAR;
	function_set.AddFunction(table_function);
	return function_set;
}

//===--------------------------------------------------------------------===//
// Paimon Metadata Function
//===--------------------------------------------------------------------===//

struct PaimonMetaDataBindData : public TableFunctionData {
	string table_location;
	PaimonOptions options;
	unique_ptr<PaimonTableMetadata> metadata;
};

struct PaimonMetaDataGlobalState : public GlobalTableFunctionState {
public:
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<PaimonMetaDataGlobalState>();
	}
	bool done = false;
};

static unique_ptr<FunctionData> PaimonMetaDataBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto ret = make_uniq<PaimonMetaDataBindData>();

	FileSystem &fs = FileSystem::GetFileSystem(context);
	ret->table_location = IcebergUtils::GetStorageLocation(context, input.inputs[0].ToString());

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		if (loption == "metadata_compression_codec") {
			ret->options.metadata_compression_codec = StringValue::Get(kv.second);
		} else if (loption == "version") {
			ret->options.table_version = StringValue::Get(kv.second);
		}
	}

	auto paimon_meta_path = PaimonTableMetadata::GetMetaDataPath(context, ret->table_location, fs, ret->options);
	ret->metadata = PaimonTableMetadata::Parse(paimon_meta_path, fs, ret->options.metadata_compression_codec);

	// Output schema: table metadata summary
	names.emplace_back("table_location");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("format_version");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_id");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("num_fields");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("partition_keys");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("primary_keys");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("num_snapshots");
	return_types.emplace_back(LogicalType::INTEGER);

	return std::move(ret);
}

static void PaimonMetaDataFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<PaimonMetaDataBindData>();
	auto &global_state = data.global_state->Cast<PaimonMetaDataGlobalState>();

	if (global_state.done) {
		output.SetCardinality(0);
		return;
	}

	auto &meta = bind_data.metadata;

	output.data[0].SetValue(0, Value(bind_data.table_location));
	output.data[1].SetValue(0, Value(meta->table_format_version));
	output.data[2].SetValue(0, Value::INTEGER(meta->schema ? meta->schema->id : -1));
	output.data[3].SetValue(0, Value::INTEGER(meta->schema ? (int32_t)meta->schema->fields.size() : 0));

	// Partition keys as comma-separated string
	string partition_keys_str;
	if (meta->schema) {
		for (idx_t i = 0; i < meta->schema->partition_keys.size(); i++) {
			if (i > 0) partition_keys_str += ", ";
			partition_keys_str += meta->schema->partition_keys[i];
		}
	}
	output.data[4].SetValue(0, Value(partition_keys_str));

	// Primary keys as comma-separated string
	string primary_keys_str;
	if (meta->schema) {
		for (idx_t i = 0; i < meta->schema->primary_keys.size(); i++) {
			if (i > 0) primary_keys_str += ", ";
			primary_keys_str += meta->schema->primary_keys[i];
		}
	}
	output.data[5].SetValue(0, Value(primary_keys_str));

	output.data[6].SetValue(0, Value::INTEGER((int32_t)meta->snapshots.size()));

	output.SetCardinality(1);
	global_state.done = true;
}

TableFunctionSet PaimonFunctions::GetPaimonMetadataFunction() {
	TableFunctionSet function_set("paimon_metadata");
	TableFunction table_function({LogicalType::VARCHAR}, PaimonMetaDataFunction, PaimonMetaDataBind,
	                             PaimonMetaDataGlobalState::Init);
	table_function.named_parameters["metadata_compression_codec"] = LogicalType::VARCHAR;
	table_function.named_parameters["version"] = LogicalType::VARCHAR;
	function_set.AddFunction(table_function);
	return function_set;
}

//===--------------------------------------------------------------------===//
// Paimon Attach Function
//===--------------------------------------------------------------------===//

struct PaimonAttachBindData : public TableFunctionData {
	string warehouse_location;
	vector<string> table_paths;
};

static unique_ptr<FunctionData> PaimonAttachBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<PaimonAttachBindData>();
	bind_data->warehouse_location = IcebergUtils::GetStorageLocation(context, input.inputs[0].ToString());

	FileSystem &fs = FileSystem::GetFileSystem(context);

	// Scan warehouse for tables (directories with snapshot/ subdirectory)
	try {
		fs.ListFiles(bind_data->warehouse_location, [&](const string &name, bool is_dir) {
			if (is_dir && !name.empty() && name[0] != '.') {
				string table_path = bind_data->warehouse_location + "/" + name;
				if (fs.DirectoryExists(table_path + "/snapshot")) {
					bind_data->table_paths.push_back(table_path);
				}
			}
		});
	} catch (const std::exception &e) {
		// Empty warehouse
	}

	names = {"table_name", "table_path", "has_snapshot", "has_schema", "has_manifest"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN,
	                LogicalType::BOOLEAN};

	return std::move(bind_data);
}

struct PaimonAttachGlobalState : public GlobalTableFunctionState {
	static unique_ptr<GlobalTableFunctionState> Init(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<PaimonAttachGlobalState>();
	}
	bool done = false;
};

static void PaimonAttachExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<PaimonAttachBindData>();
	auto &global_state = data.global_state->Cast<PaimonAttachGlobalState>();

	if (global_state.done) {
		output.SetCardinality(0);
		return;
	}

	FileSystem &fs = FileSystem::GetFileSystem(context);
	idx_t row_count = 0;

	for (const auto &table_path : bind_data.table_paths) {
		if (row_count >= STANDARD_VECTOR_SIZE) {
			break;
		}

		size_t last_slash = table_path.find_last_of('/');
		string table_name = (last_slash != string::npos) ? table_path.substr(last_slash + 1) : table_path;

		bool has_snapshot = fs.DirectoryExists(table_path + "/snapshot");
		bool has_schema = fs.DirectoryExists(table_path + "/schema");
		bool has_manifest = fs.DirectoryExists(table_path + "/manifest");

		output.data[0].SetValue(row_count, Value(table_name));
		output.data[1].SetValue(row_count, Value(table_path));
		output.data[2].SetValue(row_count, Value::BOOLEAN(has_snapshot));
		output.data[3].SetValue(row_count, Value::BOOLEAN(has_schema));
		output.data[4].SetValue(row_count, Value::BOOLEAN(has_manifest));

		row_count++;
	}

	output.SetCardinality(row_count);
	global_state.done = true;
}

TableFunctionSet PaimonFunctions::GetPaimonAttachFunction() {
	TableFunctionSet function_set("paimon_attach");
	TableFunction table_function({LogicalType::VARCHAR}, PaimonAttachExecute, PaimonAttachBind,
	                             PaimonAttachGlobalState::Init);
	function_set.AddFunction(table_function);
	return function_set;
}

//===--------------------------------------------------------------------===//
// GetTableFunctions - Register all Paimon table functions
//===--------------------------------------------------------------------===//

vector<TableFunctionSet> PaimonFunctions::GetTableFunctions(ExtensionLoader &loader) {
	vector<TableFunctionSet> functions;

	functions.push_back(GetPaimonSnapshotsFunction());
	functions.push_back(GetPaimonScanFunction(loader));
	functions.push_back(GetPaimonMetadataFunction());
	functions.push_back(GetPaimonAttachFunction());

	return functions;
}

// Simple test function implementation
void PaimonFunctions::PaimonTestFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetValue(0, Value("Paimon extension is loaded!"));
}

} // namespace duckdb
