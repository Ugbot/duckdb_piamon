#include "storage/paimon_insert.hpp"
#include "storage/paimon_catalog.hpp"
#include "storage/paimon_table_entry.hpp"
#include "paimon_metadata.hpp"

#include "duckdb/execution/execution_context.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/database.hpp"

#include <fstream>
#include <ctime>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Global State
//===--------------------------------------------------------------------===//

class PaimonInsertGlobalState : public GlobalSinkState {
public:
	PaimonInsertGlobalState() : insert_count(0) {
	}

	vector<PaimonWrittenFile> written_files;
	idx_t insert_count;
};

//===--------------------------------------------------------------------===//
// Constructor
//===--------------------------------------------------------------------===//

PaimonInsert::PaimonInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
                           physical_index_vector_t<idx_t> column_index_map_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 1), table(&table),
      column_index_map(std::move(column_index_map_p)) {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

bool PaimonInsert::IsSink() const {
	return true;
}

bool PaimonInsert::ParallelSink() const {
	return false;
}

unique_ptr<GlobalSinkState> PaimonInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<PaimonInsertGlobalState>();
}

SinkResultType PaimonInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<PaimonInsertGlobalState>();

	// The upstream PhysicalCopyToFile writes parquet files and sends us the results
	// Each row has: file_path (VARCHAR), row_count (BIGINT), file_size (BIGINT), ...
	for (idx_t r = 0; r < chunk.size(); r++) {
		PaimonWrittenFile written_file;
		written_file.file_path = chunk.GetValue(0, r).GetValue<string>();
		written_file.row_count = static_cast<int64_t>(chunk.GetValue(1, r).GetValue<idx_t>());
		written_file.file_size = static_cast<int64_t>(chunk.GetValue(2, r).GetValue<idx_t>());
		written_file.bucket = 0; // Default bucket for non-partitioned tables

		global_state.insert_count += written_file.row_count;
		global_state.written_files.push_back(std::move(written_file));
	}

	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source (returns row count)
//===--------------------------------------------------------------------===//

SourceResultType PaimonInsert::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &global_state = sink_state->Cast<PaimonInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(global_state.insert_count));
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize — create manifest + snapshot
//===--------------------------------------------------------------------===//

SinkFinalizeType PaimonInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                        OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<PaimonInsertGlobalState>();

	if (global_state.written_files.empty()) {
		return SinkFinalizeType::READY;
	}

	// Get the table path from the table entry
	string table_path = table->name;

	CommitWrittenFiles(context, table_path, global_state.written_files, global_state.insert_count);

	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Commit: manifest file + manifest list + snapshot
//===--------------------------------------------------------------------===//

void PaimonInsert::CommitWrittenFiles(ClientContext &context, const string &table_path,
                                      const vector<PaimonWrittenFile> &written_files, idx_t total_rows) const {
	FileSystem &fs = FileSystem::GetFileSystem(context);
	FileStorePathFactory path_factory(table_path, 1);

	// Ensure directories exist
	string manifest_dir = table_path + "/manifest";
	string snapshot_dir = table_path + "/snapshot";
	if (!fs.DirectoryExists(manifest_dir)) {
		fs.CreateDirectory(manifest_dir);
	}
	if (!fs.DirectoryExists(snapshot_dir)) {
		fs.CreateDirectory(snapshot_dir);
	}

	string manifest_uuid = UUID::ToString(UUID::GenerateRandomUUID());
	string manifest_list_uuid = UUID::ToString(UUID::GenerateRandomUUID());

	// Step 1: Create manifest file (Avro) containing ManifestEntry rows
	string manifest_file_path = path_factory.manifestFilePath(manifest_uuid, 0);
	{
		// Build manifest entries as a SQL query, then COPY to Avro
		string entries_sql;
		for (idx_t i = 0; i < written_files.size(); i++) {
			if (i > 0) {
				entries_sql += " UNION ALL ";
			}

			// Extract just the filename from the full path
			auto &wf = written_files[i];
			string relative_path = wf.file_path;
			// If the path starts with the table path, make it relative
			if (StringUtil::StartsWith(relative_path, table_path)) {
				relative_path = relative_path.substr(table_path.length());
				if (StringUtil::StartsWith(relative_path, "/")) {
					relative_path = relative_path.substr(1);
				}
			}

			entries_sql += "SELECT ";
			entries_sql += "CAST(0 AS TINYINT) AS _KIND, ";                           // ADD
			entries_sql += "CAST('' AS BLOB) AS _PARTITION, ";                        // Empty for non-partitioned
			entries_sql += "CAST(" + std::to_string(wf.bucket) + " AS INTEGER) AS _BUCKET, ";
			entries_sql += "CAST(1 AS INTEGER) AS _TOTAL_BUCKETS, ";
			entries_sql += "ROW(";
			entries_sql += "'" + relative_path + "', ";                                // _FILE_NAME
			entries_sql += "CAST(" + std::to_string(wf.file_size) + " AS BIGINT), ";  // _FILE_SIZE
			entries_sql += "CAST(" + std::to_string(wf.row_count) + " AS BIGINT), ";  // _ROW_COUNT
			entries_sql += "CAST('' AS BLOB), ";                                       // _MIN_KEY
			entries_sql += "CAST('' AS BLOB), ";                                       // _MAX_KEY
			entries_sql += "CAST('' AS BLOB), ";                                       // _KEY_STATS
			entries_sql += "CAST('' AS BLOB), ";                                       // _VALUE_STATS
			entries_sql += "CAST(0 AS BIGINT), ";                                      // _MIN_SEQUENCE_NUMBER
			entries_sql += "CAST(0 AS BIGINT), ";                                      // _MAX_SEQUENCE_NUMBER
			entries_sql += "CAST(0 AS BIGINT), ";                                      // _SCHEMA_ID
			entries_sql += "CAST(0 AS INTEGER), ";                                     // _LEVEL
			entries_sql += "CAST([] AS VARCHAR[]), ";                                   // _EXTRA_FILES
			entries_sql += "CAST(NULL AS TIMESTAMP), ";                                // _CREATION_TIME
			entries_sql += "CAST(NULL AS BIGINT), ";                                   // _DELETE_ROW_COUNT
			entries_sql += "CAST(NULL AS BLOB), ";                                     // _EMBEDDED_FILE_INDEX
			entries_sql += "CAST(NULL AS TINYINT), ";                                  // _FILE_SOURCE
			entries_sql += "CAST(NULL AS VARCHAR[]), ";                                // _VALUE_STATS_COLS
			entries_sql += "CAST(NULL AS VARCHAR), ";                                  // _EXTERNAL_PATH
			entries_sql += "CAST(NULL AS BIGINT), ";                                   // _FIRST_ROW_ID
			entries_sql += "CAST(NULL AS VARCHAR[])";                                  // _WRITE_COLS
			entries_sql += ") AS _FILE";
		}

		string copy_sql = "COPY (" + entries_sql + ") TO '" + manifest_file_path + "' (FORMAT AVRO)";
		auto result = context.Query(copy_sql, false);
		if (!result || result->HasError()) {
			throw IOException("Failed to write Paimon manifest file: " +
			                  (result ? result->GetError() : "unknown error"));
		}
	}

	// Get actual manifest file size
	int64_t manifest_file_size = 0;
	if (fs.FileExists(manifest_file_path)) {
		auto handle = fs.OpenFile(manifest_file_path, FileFlags::FILE_FLAGS_READ);
		manifest_file_size = handle->GetFileSize();
	}

	// Step 2: Create delta manifest list (Avro) referencing the manifest file
	string manifest_list_path = path_factory.manifestListFilePath(manifest_list_uuid, 0);
	{
		// Extract just the manifest filename
		string manifest_filename = manifest_file_path;
		size_t last_slash = manifest_filename.find_last_of('/');
		if (last_slash != string::npos) {
			manifest_filename = manifest_filename.substr(last_slash + 1);
		}

		string list_sql = "SELECT ";
		list_sql += "'" + manifest_filename + "' AS _FILE_NAME, ";
		list_sql += "CAST(" + std::to_string(manifest_file_size) + " AS BIGINT) AS _FILE_SIZE, ";
		list_sql += "CAST(" + std::to_string(written_files.size()) + " AS BIGINT) AS _NUM_ADDED_FILES, ";
		list_sql += "CAST(0 AS BIGINT) AS _NUM_DELETED_FILES, ";
		list_sql += "CAST(NULL AS BLOB) AS _PARTITION_STATS, ";
		list_sql += "CAST(0 AS BIGINT) AS _SCHEMA_ID";

		string copy_sql = "COPY (" + list_sql + ") TO '" + manifest_list_path + "' (FORMAT AVRO)";
		auto result = context.Query(copy_sql, false);
		if (!result || result->HasError()) {
			throw IOException("Failed to write Paimon manifest list file: " +
			                  (result ? result->GetError() : "unknown error"));
		}
	}

	// Get manifest list file size
	int64_t manifest_list_size = 0;
	if (fs.FileExists(manifest_list_path)) {
		auto handle = fs.OpenFile(manifest_list_path, FileFlags::FILE_FLAGS_READ);
		manifest_list_size = handle->GetFileSize();
	}

	// Step 3: Determine next snapshot ID
	int64_t next_snapshot_id = 1;
	string latest_file = table_path + "/snapshot/LATEST";
	if (fs.FileExists(latest_file)) {
		try {
			auto handle = fs.OpenFile(latest_file, FileFlags::FILE_FLAGS_READ);
			auto file_size = handle->GetFileSize();
			string content(file_size, '\0');
			handle->Read(&content[0], file_size);
			// Content is like "snapshot-N"
			if (StringUtil::StartsWith(content, "snapshot-")) {
				next_snapshot_id = std::stoll(content.substr(9)) + 1;
			}
		} catch (...) {
			// If we can't read LATEST, start at 1
		}
	}

	// Extract manifest list filename for JSON
	string manifest_list_filename = manifest_list_path;
	{
		size_t last_slash = manifest_list_filename.find_last_of('/');
		if (last_slash != string::npos) {
			manifest_list_filename = manifest_list_filename.substr(last_slash + 1);
		}
	}

	// Step 4: Write snapshot JSON
	auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
	    std::chrono::system_clock::now().time_since_epoch()).count();

	string snapshot_json = "{\n";
	snapshot_json += "  \"version\": 3,\n";
	snapshot_json += "  \"id\": " + std::to_string(next_snapshot_id) + ",\n";
	snapshot_json += "  \"schemaId\": 0,\n";
	snapshot_json += "  \"baseManifestList\": \"\",\n";
	snapshot_json += "  \"deltaManifestList\": \"" + manifest_list_filename + "\",\n";
	snapshot_json += "  \"deltaManifestListSize\": " + std::to_string(manifest_list_size) + ",\n";
	snapshot_json += "  \"changelogManifestList\": null,\n";
	snapshot_json += "  \"indexManifest\": null,\n";
	snapshot_json += "  \"commitUser\": \"duckdb-paimon\",\n";
	snapshot_json += "  \"commitIdentifier\": 9223372036854775807,\n";
	snapshot_json += "  \"commitKind\": \"APPEND\",\n";
	snapshot_json += "  \"timeMillis\": " + std::to_string(now_ms) + ",\n";
	snapshot_json += "  \"logOffsets\": {},\n";
	snapshot_json += "  \"totalRecordCount\": " + std::to_string(total_rows) + ",\n";
	snapshot_json += "  \"deltaRecordCount\": " + std::to_string(total_rows) + ",\n";
	snapshot_json += "  \"changelogRecordCount\": 0,\n";
	snapshot_json += "  \"watermark\": -9223372036854775808\n";
	snapshot_json += "}";

	string snapshot_path = path_factory.snapshotFilePath(next_snapshot_id);
	{
		auto handle = fs.OpenFile(snapshot_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		handle->Write((void *)snapshot_json.c_str(), snapshot_json.size());
	}

	// Step 5: Update LATEST pointer
	{
		string latest_content = "snapshot-" + std::to_string(next_snapshot_id);
		// Remove old file first if it exists
		if (fs.FileExists(latest_file)) {
			fs.RemoveFile(latest_file);
		}
		auto handle = fs.OpenFile(latest_file, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		handle->Write((void *)latest_content.c_str(), latest_content.size());
	}

	// Create EARLIEST if it doesn't exist
	string earliest_file = table_path + "/snapshot/EARLIEST";
	if (!fs.FileExists(earliest_file)) {
		string earliest_content = "snapshot-" + std::to_string(next_snapshot_id);
		auto handle = fs.OpenFile(earliest_file, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		handle->Write((void *)earliest_content.c_str(), earliest_content.size());
	}
}

//===--------------------------------------------------------------------===//
// Plan COPY TO FILE for writing parquet
//===--------------------------------------------------------------------===//

PhysicalOperator &PaimonInsert::PlanCopyForInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   const string &data_path, const vector<string> &names,
                                                   const vector<LogicalType> &types,
                                                   optional_ptr<PhysicalOperator> plan) {
	auto &instance = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(instance);
	auto data = CatalogTransaction::GetSystemTransaction(instance);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto copy_entry = schema.GetEntry(data, CatalogType::COPY_FUNCTION_ENTRY, "parquet");
	if (!copy_entry) {
		throw MissingExtensionException("Parquet extension required for Paimon INSERT");
	}
	auto &copy_fun = copy_entry->Cast<CopyFunctionCatalogEntry>();

	vector<string> names_copy = names;
	vector<LogicalType> types_copy = types;

	CopyInfo copy_info;
	copy_info.file_path = data_path;
	copy_info.format = "parquet";
	copy_info.is_from = false;
	CopyFunctionBindInput bind_input(copy_info);
	auto function_data = copy_fun.function.copy_to_bind(context, bind_input, names_copy, types_copy);

	auto &physical_copy = planner.Make<PhysicalCopyToFile>(
	    GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS), copy_fun.function,
	    std::move(function_data), 1);

	auto &copy_ref = physical_copy.Cast<PhysicalCopyToFile>();
	copy_ref.use_tmp_file = false;
	copy_ref.filename_pattern.SetFilenamePattern("data-{uuidv7}");
	copy_ref.file_path = data_path;
	copy_ref.partition_output = false;
	copy_ref.write_empty_file = false;
	copy_ref.rotate = true;
	copy_ref.file_extension = "parquet";
	copy_ref.overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
	copy_ref.per_thread_output = false;
	copy_ref.return_type = CopyFunctionReturnType::WRITTEN_FILE_STATISTICS;
	copy_ref.children.push_back(*plan);
	copy_ref.names = names;
	copy_ref.expected_types = types;

	return physical_copy;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//

string PaimonInsert::GetName() const {
	return "PAIMON_INSERT";
}

InsertionOrderPreservingMap<string> PaimonInsert::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	if (table) {
		result["Table Name"] = table->name;
	}
	return result;
}

} // namespace duckdb
