#include "storage/paimon_insert.hpp"
#include "storage/paimon_catalog.hpp"
#include "storage/paimon_table_entry.hpp"
#include "paimon_metadata.hpp"
#include "paimon_manifest.hpp"
#include "paimon_avro_writer.hpp"
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
// Avro schemas + Value builders for native manifest writing (see PaimonAvroWriter).
//===--------------------------------------------------------------------===//

// SimpleStats sub-record (reused for _PARTITION_STATS / _KEY_STATS / _VALUE_STATS).
static const char *SIMPLE_STATS_FIELDS =
    "{\"name\":\"_MIN_VALUES\",\"type\":\"bytes\"},"
    "{\"name\":\"_MAX_VALUES\",\"type\":\"bytes\"},"
    "{\"name\":\"_NULL_COUNTS\",\"type\":{\"type\":\"array\",\"items\":\"long\"}}";

static string ManifestListSchemaJson() {
	return string("{\"type\":\"record\",\"name\":\"manifest_list\",\"fields\":["
	              "{\"name\":\"_FILE_NAME\",\"type\":\"string\"},"
	              "{\"name\":\"_FILE_SIZE\",\"type\":\"long\"},"
	              "{\"name\":\"_NUM_ADDED_FILES\",\"type\":\"long\"},"
	              "{\"name\":\"_NUM_DELETED_FILES\",\"type\":\"long\"},"
	              "{\"name\":\"_PARTITION_STATS\",\"type\":{\"type\":\"record\",\"name\":\"part_stats\","
	              "\"fields\":[") +
	       SIMPLE_STATS_FIELDS +
	       "]}},"
	       "{\"name\":\"_SCHEMA_ID\",\"type\":\"long\"}]}";
}

static string ManifestEntrySchemaJson() {
	string ks = string("{\"type\":\"record\",\"name\":\"key_stats\",\"fields\":[") + SIMPLE_STATS_FIELDS + "]}";
	string vs = string("{\"type\":\"record\",\"name\":\"value_stats\",\"fields\":[") + SIMPLE_STATS_FIELDS + "]}";
	return string("{\"type\":\"record\",\"name\":\"manifest_entry\",\"fields\":["
	              "{\"name\":\"_KIND\",\"type\":\"int\"},"
	              "{\"name\":\"_PARTITION\",\"type\":\"bytes\"},"
	              "{\"name\":\"_BUCKET\",\"type\":\"int\"},"
	              "{\"name\":\"_TOTAL_BUCKETS\",\"type\":\"int\"},"
	              "{\"name\":\"_FILE\",\"type\":{\"type\":\"record\",\"name\":\"data_file\",\"fields\":["
	              "{\"name\":\"_FILE_NAME\",\"type\":\"string\"},"
	              "{\"name\":\"_FILE_SIZE\",\"type\":\"long\"},"
	              "{\"name\":\"_ROW_COUNT\",\"type\":\"long\"},"
	              "{\"name\":\"_MIN_KEY\",\"type\":\"bytes\"},"
	              "{\"name\":\"_MAX_KEY\",\"type\":\"bytes\"},"
	              "{\"name\":\"_KEY_STATS\",\"type\":") +
	       ks +
	       "},"
	       "{\"name\":\"_VALUE_STATS\",\"type\":" +
	       vs +
	       "},"
	       "{\"name\":\"_MIN_SEQUENCE_NUMBER\",\"type\":\"long\"},"
	       "{\"name\":\"_MAX_SEQUENCE_NUMBER\",\"type\":\"long\"},"
	       "{\"name\":\"_SCHEMA_ID\",\"type\":\"long\"},"
	       "{\"name\":\"_LEVEL\",\"type\":\"int\"},"
	       "{\"name\":\"_EXTRA_FILES\",\"type\":{\"type\":\"array\",\"items\":\"string\"}},"
	       "{\"name\":\"_CREATION_TIME\",\"type\":[\"null\",\"long\"]},"
	       "{\"name\":\"_DELETE_ROW_COUNT\",\"type\":[\"null\",\"long\"]},"
	       "{\"name\":\"_EMBEDDED_FILE_INDEX\",\"type\":[\"null\",\"bytes\"]},"
	       "{\"name\":\"_FILE_SOURCE\",\"type\":[\"null\",\"int\"]},"
	       "{\"name\":\"_VALUE_STATS_COLS\",\"type\":[\"null\",{\"type\":\"array\",\"items\":\"string\"}]},"
	       "{\"name\":\"_EXTERNAL_PATH\",\"type\":[\"null\",\"string\"]}"
	       "]}}]}";
}

static Value EmptyBytes() {
	return Value::BLOB((const_data_ptr_t) "", 0);
}

// SimpleStats value: 12 zero bytes for min/max (matches Paimon's empty-row encoding), no null counts.
static Value SimpleStatsValue() {
	string zeros(12, '\0');
	child_list_t<Value> kv;
	kv.emplace_back("_MIN_VALUES", Value::BLOB((const_data_ptr_t)zeros.data(), 12));
	kv.emplace_back("_MAX_VALUES", Value::BLOB((const_data_ptr_t)zeros.data(), 12));
	kv.emplace_back("_NULL_COUNTS", Value::LIST(LogicalType::BIGINT, vector<Value>()));
	return Value::STRUCT(std::move(kv));
}

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
                                      const vector<PaimonWrittenFile> &written_files, idx_t total_rows,
                                      bool carry_forward, const string &commit_kind) {
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

	// Step 1: Write the manifest file (Avro) natively — one ManifestEntry record per added data file.
	string manifest_file_path = path_factory.manifestFilePath(manifest_uuid, 0);
	{
		vector<vector<Value>> entry_rows;
		for (auto &wf : written_files) {
			// Manifests store only the data file's basename; readers reconstruct the full path from
			// the table location + partition dir + bucket-N.
			string relative_path = wf.file_path;
			size_t last_slash = relative_path.find_last_of('/');
			if (last_slash != string::npos) {
				relative_path = relative_path.substr(last_slash + 1);
			}
			child_list_t<Value> file;
			file.emplace_back("_FILE_NAME", Value(relative_path));
			file.emplace_back("_FILE_SIZE", Value::BIGINT(wf.file_size));
			file.emplace_back("_ROW_COUNT", Value::BIGINT(wf.row_count));
			file.emplace_back("_MIN_KEY", EmptyBytes());
			file.emplace_back("_MAX_KEY", EmptyBytes());
			file.emplace_back("_KEY_STATS", SimpleStatsValue());
			file.emplace_back("_VALUE_STATS", SimpleStatsValue());
			file.emplace_back("_MIN_SEQUENCE_NUMBER", Value::BIGINT(0));
			file.emplace_back("_MAX_SEQUENCE_NUMBER", Value::BIGINT(0));
			file.emplace_back("_SCHEMA_ID", Value::BIGINT(0));
			file.emplace_back("_LEVEL", Value::INTEGER(0));
			file.emplace_back("_EXTRA_FILES", Value::LIST(LogicalType::VARCHAR, vector<Value>()));
			file.emplace_back("_CREATION_TIME", Value(LogicalType::BIGINT));
			file.emplace_back("_DELETE_ROW_COUNT", Value(LogicalType::BIGINT));
			file.emplace_back("_EMBEDDED_FILE_INDEX", Value(LogicalType::BLOB));
			file.emplace_back("_FILE_SOURCE", Value(LogicalType::INTEGER));
			file.emplace_back("_VALUE_STATS_COLS", Value(LogicalType::LIST(LogicalType::VARCHAR)));
			file.emplace_back("_EXTERNAL_PATH", Value(LogicalType::VARCHAR));

			vector<Value> row;
			row.push_back(Value::INTEGER(0)); // _KIND = ADD
			row.push_back(EmptyBytes());      // _PARTITION (unpartitioned)
			row.push_back(Value::INTEGER(wf.bucket));
			row.push_back(Value::INTEGER(1)); // _TOTAL_BUCKETS
			row.push_back(Value::STRUCT(std::move(file)));
			entry_rows.push_back(std::move(row));
		}
		PaimonAvroWriter::WriteFile(context, manifest_file_path, ManifestEntrySchemaJson(), entry_rows);
	}

	// Get actual manifest file size
	int64_t manifest_file_size = 0;
	if (fs.FileExists(manifest_file_path)) {
		auto handle = fs.OpenFile(manifest_file_path, FileFlags::FILE_FLAGS_READ);
		manifest_file_size = handle->GetFileSize();
	}

	// Step 2: Write the delta manifest list (Avro) natively, referencing the new manifest file.
	string manifest_list_path = path_factory.manifestListFilePath(manifest_list_uuid, 0);
	{
		string manifest_filename = manifest_file_path;
		size_t last_slash = manifest_filename.find_last_of('/');
		if (last_slash != string::npos) {
			manifest_filename = manifest_filename.substr(last_slash + 1);
		}
		vector<Value> row;
		row.push_back(Value(manifest_filename));
		row.push_back(Value::BIGINT(manifest_file_size));
		row.push_back(Value::BIGINT((int64_t)written_files.size()));
		row.push_back(Value::BIGINT(0));
		row.push_back(SimpleStatsValue());
		row.push_back(Value::BIGINT(0));
		PaimonAvroWriter::WriteFile(context, manifest_list_path, ManifestListSchemaJson(), {row});
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

	// Step 2b: Write the BASE manifest list = all manifests carried forward from the previous snapshot
	// (its base + delta). Without this, each commit would only expose its own delta and prior data
	// would vanish. For the first snapshot (or compaction) the base list is empty.
	string base_list_path = path_factory.manifestListFilePath(manifest_list_uuid, 1);
	{
		vector<PaimonManifestFileMeta> carried;
		if (next_snapshot_id > 1 && carry_forward) {
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

		vector<vector<Value>> base_rows;
		for (auto &m : carried) {
			vector<Value> row;
			row.push_back(Value(m.file_name));
			row.push_back(Value::BIGINT(m.file_size));
			row.push_back(Value::BIGINT(m.num_added_files));
			row.push_back(Value::BIGINT(m.num_deleted_files));
			row.push_back(SimpleStatsValue());
			row.push_back(Value::BIGINT(m.schema_id));
			base_rows.push_back(std::move(row));
		}
		PaimonAvroWriter::WriteFile(context, base_list_path, ManifestListSchemaJson(), base_rows);
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
	snapshot_json += "  \"commitKind\": \"" + commit_kind + "\",\n";
	snapshot_json += "  \"timeMillis\": " + std::to_string(now_ms) + ",\n";
	snapshot_json += "  \"logOffsets\": {},\n";
	snapshot_json += "  \"totalRecordCount\": " + std::to_string(total_rows) + ",\n";
	snapshot_json += "  \"deltaRecordCount\": " + std::to_string(total_rows) + ",\n";
	snapshot_json += "  \"changelogRecordCount\": 0,\n";
	snapshot_json += "  \"watermark\": -9223372036854775808\n";
	snapshot_json += "}";

	// The snapshot file is the atomic commit point: FILE_CREATE_NEW fails if another writer already
	// committed this id, so two concurrent commits can't clobber each other.
	string snapshot_path = path_factory.snapshotFilePath(next_snapshot_id);
	{
		auto handle = fs.OpenFile(snapshot_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
		handle->Write((void *)snapshot_json.c_str(), snapshot_json.size());
	}

	// Step 5: Update the LATEST hint atomically (write a temp file, then rename over it) so a reader
	// never observes a partially written pointer. Paimon hint files hold the bare snapshot id.
	auto write_hint_atomic = [&](const string &hint_path, const string &content) {
		string tmp_path = hint_path + ".tmp-" + UUID::ToString(UUID::GenerateRandomUUID());
		{
			auto handle = fs.OpenFile(tmp_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
			handle->Write((void *)content.c_str(), content.size());
		}
		fs.MoveFile(tmp_path, hint_path); // rename is atomic on a local filesystem
	};
	write_hint_atomic(latest_file, std::to_string(next_snapshot_id));

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
