#include "storage/paimon_insert.hpp"
#include "storage/paimon_catalog.hpp"
#include "storage/paimon_table_entry.hpp"
#include "paimon_metadata.hpp"
#include "paimon_manifest.hpp"
#include "iceberg_utils.hpp"

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
#include "duckdb/main/connection.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

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
	//! Buffered rows for primary-key writes (value columns only; system columns are added at commit).
	unique_ptr<ColumnDataCollection> buffered;
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
	auto state = make_uniq<PaimonInsertGlobalState>();
	if (pk_mode) {
		state->buffered = make_uniq<ColumnDataCollection>(context, value_types);
	}
	return std::move(state);
}

SinkResultType PaimonInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<PaimonInsertGlobalState>();

	if (pk_mode) {
		// Buffer the raw rows; the data file (with system columns) is written at Finalize.
		global_state.buffered->Append(chunk);
		global_state.insert_count += chunk.size();
		return SinkResultType::NEED_MORE_INPUT;
	}

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

	// Get the absolute table directory from the Paimon table entry.
	auto &paimon_table = table->Cast<PaimonTableEntry>();
	string table_path = paimon_table.GetTablePath();

	if (pk_mode) {
		if (!global_state.buffered || global_state.buffered->Count() == 0) {
			return SinkFinalizeType::READY;
		}
		CommitPrimaryKeyRows(context, table_path, *global_state.buffered);
		return SinkFinalizeType::READY;
	}

	if (global_state.written_files.empty()) {
		return SinkFinalizeType::READY;
	}
	CommitWrittenFiles(context, table_path, global_state.written_files, global_state.insert_count);
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Primary-key write: materialize buffered rows with system columns, then commit.
//===--------------------------------------------------------------------===//

void PaimonInsert::CommitPrimaryKeyRows(ClientContext &context, const string &table_path,
                                        ColumnDataCollection &rows) const {
	FileSystem &fs = FileSystem::GetFileSystem(context);
	Connection conn(DatabaseInstance::GetDatabase(context));

	string bucket_dir = table_path + "/bucket-0";
	if (!fs.DirectoryExists(bucket_dir)) {
		fs.CreateDirectory(bucket_dir);
	}

	auto quote_id = [](const string &n) {
		string e;
		for (char c : n) {
			e += (c == '"') ? "\"\"" : string(1, c);
		}
		return "\"" + e + "\"";
	};

	// Determine the base sequence number: continue past the max sequence already in the table so
	// later commits always outrank earlier ones for the same key (Paimon merge ordering).
	int64_t base_seq = 0;
	{
		vector<string> existing;
		fs.ListFiles(bucket_dir, [&](const string &fname, bool is_dir) {
			if (!is_dir && StringUtil::EndsWith(fname, ".parquet")) {
				existing.push_back(bucket_dir + "/" + fname);
			}
		});
		if (!existing.empty()) {
			string list;
			for (idx_t i = 0; i < existing.size(); i++) {
				list += (i ? ", " : "") + ("'" + existing[i] + "'");
			}
			auto r = conn.Query("SELECT max(\"_SEQUENCE_NUMBER\") FROM read_parquet([" + list + "])");
			if (r && !r->HasError()) {
				auto v = r->Fetch();
				if (v && v->size() > 0 && !v->GetValue(0, 0).IsNull()) {
					base_seq = v->GetValue(0, 0).GetValue<int64_t>();
				}
			}
		}
	}

	// Stage the buffered value rows in a temp table, then COPY them out with Paimon's PK system
	// columns: _KEY_<pk> (one per key column), _SEQUENCE_NUMBER, _VALUE_KIND (0 = INSERT), values.
	string staged_cols;
	for (idx_t i = 0; i < value_names.size(); i++) {
		staged_cols += (i ? ", " : "") + quote_id(value_names[i]) + " " + value_types[i].ToString();
	}
	if (auto r = conn.Query("CREATE TEMP TABLE __paimon_pk_stage (" + staged_cols + ")"); r && r->HasError()) {
		throw IOException("Failed to stage Paimon PK rows: " + r->GetError());
	}
	{
		Appender appender(conn, "__paimon_pk_stage");
		for (auto &chunk : rows.Chunks()) {
			appender.AppendDataChunk(const_cast<DataChunk &>(chunk));
		}
		appender.Close();
	}

	string uuid = UUID::ToString(UUID::GenerateRandomUUID());
	string data_file = bucket_dir + "/data-" + uuid + "-0.parquet";

	string projection;
	for (auto &pk : pk_names) {
		projection += quote_id(pk) + " AS " + quote_id("_KEY_" + pk) + ", ";
	}
	projection += "CAST(" + std::to_string(base_seq) + " + row_number() OVER () AS BIGINT) AS \"_SEQUENCE_NUMBER\", ";
	projection += "CAST(0 AS TINYINT) AS \"_VALUE_KIND\"";
	for (auto &v : value_names) {
		projection += ", " + quote_id(v);
	}
	string copy_sql = "COPY (SELECT " + projection + " FROM __paimon_pk_stage) TO '" + data_file + "' (FORMAT PARQUET)";
	if (auto r = conn.Query(copy_sql); r && r->HasError()) {
		conn.Query("DROP TABLE __paimon_pk_stage");
		throw IOException("Failed to write Paimon PK data file: " + r->GetError());
	}
	conn.Query("DROP TABLE __paimon_pk_stage");

	PaimonWrittenFile wf;
	wf.file_path = data_file;
	wf.row_count = (int64_t)rows.Count();
	wf.bucket = 0;
	if (fs.FileExists(data_file)) {
		auto h = fs.OpenFile(data_file, FileFlags::FILE_FLAGS_READ);
		wf.file_size = h->GetFileSize();
	}
	vector<PaimonWrittenFile> written {wf};
	CommitWrittenFiles(context, table_path, written, rows.Count());
}

//===--------------------------------------------------------------------===//
// Commit: manifest file + manifest list + snapshot
//===--------------------------------------------------------------------===//

void PaimonInsert::CommitWrittenFiles(ClientContext &context, const string &table_path,
                                      const vector<PaimonWrittenFile> &written_files, idx_t total_rows) {
	FileSystem &fs = FileSystem::GetFileSystem(context);
	FileStorePathFactory path_factory(table_path, 1);

	// Avro writes (COPY ... TO ... (FORMAT AVRO)) must run on a SEPARATE connection: issuing them on
	// the current context during Finalize re-enters the engine and deadlocks.
	Connection conn(DatabaseInstance::GetDatabase(context));

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

			// Paimon manifests store only the data file's basename. The full path is reconstructed
			// by readers from the table location + partition dir + bucket-N + this name, so storing a
			// "bucket-0/..."-prefixed path would double the bucket directory.
			auto &wf = written_files[i];
			string relative_path = wf.file_path;
			size_t last_slash = relative_path.find_last_of('/');
			if (last_slash != string::npos) {
				relative_path = relative_path.substr(last_slash + 1);
			}

			// Build a named STRUCT for _FILE so the Avro record carries the spec field names
			// (_FILE_NAME, _FILE_SIZE, ...) that readers match on.
			entries_sql += "SELECT ";
			entries_sql += "CAST(0 AS INTEGER) AS _KIND, ";                           // ADD
			entries_sql += "CAST('' AS BLOB) AS _PARTITION, ";                        // Empty for non-partitioned
			entries_sql += "CAST(" + std::to_string(wf.bucket) + " AS INTEGER) AS _BUCKET, ";
			entries_sql += "CAST(1 AS INTEGER) AS _TOTAL_BUCKETS, ";
			entries_sql += "{";
			entries_sql += "'_FILE_NAME': '" + relative_path + "', ";
			entries_sql += "'_FILE_SIZE': CAST(" + std::to_string(wf.file_size) + " AS BIGINT), ";
			entries_sql += "'_ROW_COUNT': CAST(" + std::to_string(wf.row_count) + " AS BIGINT), ";
			// Empty SimpleStats (min/max = empty BinaryRow = 12 zero bytes; no null counts). Readers
			// require these to be records, not blobs.
			string empty_stats = "{'_MIN_VALUES': "
			                     "'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'::BLOB, "
			                     "'_MAX_VALUES': "
			                     "'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'::BLOB, "
			                     "'_NULL_COUNTS': CAST([] AS BIGINT[])}";
			entries_sql += "'_MIN_KEY': CAST('' AS BLOB), ";
			entries_sql += "'_MAX_KEY': CAST('' AS BLOB), ";
			entries_sql += "'_KEY_STATS': " + empty_stats + ", ";
			entries_sql += "'_VALUE_STATS': " + empty_stats + ", ";
			entries_sql += "'_MIN_SEQUENCE_NUMBER': CAST(0 AS BIGINT), ";
			entries_sql += "'_MAX_SEQUENCE_NUMBER': CAST(0 AS BIGINT), ";
			entries_sql += "'_SCHEMA_ID': CAST(0 AS BIGINT), ";
			entries_sql += "'_LEVEL': CAST(0 AS INTEGER), ";
			entries_sql += "'_EXTRA_FILES': CAST([] AS VARCHAR[]), ";
			entries_sql += "'_CREATION_TIME': CAST(NULL AS BIGINT), ";
			entries_sql += "'_DELETE_ROW_COUNT': CAST(NULL AS BIGINT), ";
			entries_sql += "'_EMBEDDED_FILE_INDEX': CAST(NULL AS BLOB), ";
			entries_sql += "'_FILE_SOURCE': CAST(NULL AS INTEGER), ";
			entries_sql += "'_VALUE_STATS_COLS': CAST(NULL AS VARCHAR[]), ";
			entries_sql += "'_EXTERNAL_PATH': CAST(NULL AS VARCHAR)";
			entries_sql += "} AS _FILE";
		}

		string copy_sql = "COPY (" + entries_sql + ") TO '" + manifest_file_path + "' (FORMAT AVRO)";
		auto result = conn.Query(copy_sql);
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
		// _PARTITION_STATS must be a non-null SimpleStats record. For an unpartitioned table the
		// partition is an empty BinaryRow (12 zero bytes, matching Paimon's own output) and there are
		// no per-partition null counts.
		list_sql += "{'_MIN_VALUES': '\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'::BLOB, "
		            "'_MAX_VALUES': '\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'::BLOB, "
		            "'_NULL_COUNTS': CAST([] AS BIGINT[])} AS _PARTITION_STATS, ";
		list_sql += "CAST(0 AS BIGINT) AS _SCHEMA_ID";

		string copy_sql = "COPY (" + list_sql + ") TO '" + manifest_list_path + "' (FORMAT AVRO)";
		auto result = conn.Query(copy_sql);
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

	// Determine the next snapshot id from the LATEST hint (bare id, with legacy "snapshot-N" tolerated).
	int64_t next_snapshot_id = 1;
	string latest_file = table_path + "/snapshot/LATEST";
	if (fs.FileExists(latest_file)) {
		try {
			auto handle = fs.OpenFile(latest_file, FileFlags::FILE_FLAGS_READ);
			auto file_size = handle->GetFileSize();
			string content(file_size, '\0');
			handle->Read(&content[0], file_size);
			StringUtil::Trim(content);
			if (StringUtil::StartsWith(content, "snapshot-")) {
				content = content.substr(9);
			}
			if (!content.empty()) {
				next_snapshot_id = std::stoll(content) + 1;
			}
		} catch (...) {
			// If we can't read LATEST, start at 1
		}
	}

	const char *PARTITION_STATS_STRUCT =
	    "{'_MIN_VALUES': '\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'::BLOB, "
	    "'_MAX_VALUES': '\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'::BLOB, "
	    "'_NULL_COUNTS': CAST([] AS BIGINT[])}";

	// Step 2b: Build the BASE manifest list = all manifests carried forward from the previous
	// snapshot (its base + delta). Without this, each commit would only expose its own delta and
	// previously-written data would vanish. For the first snapshot the base list is empty.
	string base_list_path = path_factory.manifestListFilePath(manifest_list_uuid, 1);
	{
		vector<PaimonManifestFileMeta> carried;
		if (next_snapshot_id > 1) {
			string prev_snapshot_path = table_path + "/snapshot/snapshot-" + std::to_string(next_snapshot_id - 1);
			try {
				string prev_json = IcebergUtils::FileToString(prev_snapshot_path, fs);
				auto doc = unique_ptr<yyjson_doc, YyjsonDocDeleter>(
				    yyjson_read(prev_json.c_str(), prev_json.size(), 0));
				if (doc) {
					auto root = yyjson_doc_get_root(doc.get());
					for (const char *key : {"baseManifestList", "deltaManifestList"}) {
						auto v = yyjson_obj_get(root, key);
						if (v && yyjson_is_str(v)) {
							string list_name = yyjson_get_str(v);
							if (!list_name.empty()) {
								auto metas = ReadPaimonManifestList(context, manifest_dir + "/" + list_name);
								for (auto &m : metas) {
									carried.push_back(m);
								}
							}
						}
					}
				}
			} catch (const std::exception &e) {
				throw IOException("Failed to carry forward manifests from previous snapshot: " + string(e.what()));
			}
		}

		string list_sql;
		if (carried.empty()) {
			list_sql = "SELECT CAST('' AS VARCHAR) AS _FILE_NAME, CAST(0 AS BIGINT) AS _FILE_SIZE, "
			           "CAST(0 AS BIGINT) AS _NUM_ADDED_FILES, CAST(0 AS BIGINT) AS _NUM_DELETED_FILES, " +
			           string(PARTITION_STATS_STRUCT) + " AS _PARTITION_STATS, CAST(0 AS BIGINT) AS _SCHEMA_ID "
			           "WHERE 1=0";
		} else {
			for (idx_t i = 0; i < carried.size(); i++) {
				auto &m = carried[i];
				if (i > 0) {
					list_sql += " UNION ALL ";
				}
				list_sql += "SELECT '" + m.file_name + "' AS _FILE_NAME, " +
				            "CAST(" + std::to_string(m.file_size) + " AS BIGINT) AS _FILE_SIZE, " +
				            "CAST(" + std::to_string(m.num_added_files) + " AS BIGINT) AS _NUM_ADDED_FILES, " +
				            "CAST(" + std::to_string(m.num_deleted_files) + " AS BIGINT) AS _NUM_DELETED_FILES, " +
				            string(PARTITION_STATS_STRUCT) + " AS _PARTITION_STATS, " +
				            "CAST(" + std::to_string(m.schema_id) + " AS BIGINT) AS _SCHEMA_ID";
			}
		}
		string copy_sql = "COPY (" + list_sql + ") TO '" + base_list_path + "' (FORMAT AVRO)";
		auto result = conn.Query(copy_sql);
		if (!result || result->HasError()) {
			throw IOException("Failed to write Paimon base manifest list file: " +
			                  (result ? result->GetError() : "unknown error"));
		}
	}
	string base_list_filename = base_list_path;
	{
		size_t last_slash = base_list_filename.find_last_of('/');
		if (last_slash != string::npos) {
			base_list_filename = base_list_filename.substr(last_slash + 1);
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
	snapshot_json += "  \"baseManifestList\": \"" + base_list_filename + "\",\n";
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

	// Step 5: Update LATEST pointer. Paimon hint files hold the bare snapshot id ("1"), not the
	// file name — this is what Flink/Spark/pypaimon expect.
	{
		string latest_content = std::to_string(next_snapshot_id);
		if (fs.FileExists(latest_file)) {
			fs.RemoveFile(latest_file);
		}
		auto handle = fs.OpenFile(latest_file, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		handle->Write((void *)latest_content.c_str(), latest_content.size());
	}

	// EARLIEST always points at the oldest retained snapshot; (re)write it to the bare id.
	string earliest_file = table_path + "/snapshot/EARLIEST";
	if (!fs.FileExists(earliest_file)) {
		string earliest_content = std::to_string(next_snapshot_id);
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
