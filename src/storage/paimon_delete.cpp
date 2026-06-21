#include "storage/paimon_delete.hpp"
#include "storage/paimon_insert.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/appender.hpp"

namespace duckdb {

PaimonDelete::PaimonDelete(PhysicalPlan &physical_plan, vector<LogicalType> types, TableCatalogEntry &table,
                           idx_t row_id_index, string table_path, vector<string> pk_names, vector<string> value_names,
                           vector<LogicalType> value_types, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      table(table), row_id_index(row_id_index), table_path(std::move(table_path)), pk_names(std::move(pk_names)),
      value_names(std::move(value_names)), value_types(std::move(value_types)) {
}

class PaimonDeleteGlobalState : public GlobalSinkState {
public:
	PaimonDeleteGlobalState(ClientContext &context, const vector<LogicalType> &key_types)
	    : keys(context, key_types), delete_count(0) {
	}
	ColumnDataCollection keys; //! buffered primary-key values (one column per key column) of deletes
	idx_t delete_count;
};

// Types of the primary-key columns, in pk order.
static vector<LogicalType> KeyTypes(const vector<string> &pk_names, const vector<string> &value_names,
                                    const vector<LogicalType> &value_types) {
	vector<LogicalType> result;
	for (auto &pk : pk_names) {
		for (idx_t i = 0; i < value_names.size(); i++) {
			if (StringUtil::CIEquals(value_names[i], pk)) {
				result.push_back(value_types[i]);
				break;
			}
		}
	}
	return result;
}

unique_ptr<GlobalSinkState> PaimonDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<PaimonDeleteGlobalState>(context, KeyTypes(pk_names, value_names, value_types));
}

SinkResultType PaimonDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<PaimonDeleteGlobalState>();
	// The row-id column carries the primary key: a scalar for a single-column PK, a STRUCT of the key
	// columns for a composite PK (see paimon_scan rowid emission).
	auto key_types = KeyTypes(pk_names, value_names, value_types);
	DataChunk keys;
	keys.InitializeEmpty(key_types);
	auto &rid = chunk.data[row_id_index];
	if (pk_names.size() == 1) {
		keys.data[0].Reference(rid);
	} else {
		auto &entries = StructVector::GetEntries(rid);
		for (idx_t j = 0; j < pk_names.size(); j++) {
			keys.data[j].Reference(*entries[j]);
		}
	}
	keys.SetCardinality(chunk.size());
	gstate.keys.Append(keys);
	gstate.delete_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

static string QId(const string &n) {
	string e;
	for (char c : n) {
		e += (c == '"') ? "\"\"" : string(1, c);
	}
	return "\"" + e + "\"";
}

SinkFinalizeType PaimonDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                        OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<PaimonDeleteGlobalState>();
	if (gstate.keys.Count() == 0) {
		return SinkFinalizeType::READY;
	}

	FileSystem &fs = FileSystem::GetFileSystem(context);
	Connection conn(DatabaseInstance::GetDatabase(context));
	string bucket_dir = table_path + "/bucket-0";
	if (!fs.DirectoryExists(bucket_dir)) {
		fs.CreateDirectory(bucket_dir);
	}

	// Continue sequence numbers past the table's current max so tombstones outrank existing rows.
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

	// Stage the deleted keys (one column per key column), then COPY out tombstone rows: _KEY_<pk>,
	// _SEQUENCE_NUMBER, _VALUE_KIND = 3 (DELETE), the pk value columns, NULL for the rest.
	auto key_types = KeyTypes(pk_names, value_names, value_types);
	auto key_col = [&](idx_t j) { return "k" + std::to_string(j); };
	string stage_cols;
	for (idx_t j = 0; j < pk_names.size(); j++) {
		stage_cols += (j ? ", " : "") + key_col(j) + " " + key_types[j].ToString();
	}
	if (auto r = conn.Query("CREATE TEMP TABLE __paimon_del_keys (" + stage_cols + ")"); r && r->HasError()) {
		throw IOException("Failed to stage Paimon delete keys: " + r->GetError());
	}
	{
		Appender appender(conn, "__paimon_del_keys");
		for (auto &chunk : gstate.keys.Chunks()) {
			appender.AppendDataChunk(const_cast<DataChunk &>(chunk));
		}
		appender.Close();
	}

	auto key_index = [&](const string &col) -> int {
		for (idx_t j = 0; j < pk_names.size(); j++) {
			if (StringUtil::CIEquals(pk_names[j], col)) {
				return (int)j;
			}
		}
		return -1;
	};
	string projection;
	for (idx_t j = 0; j < pk_names.size(); j++) {
		projection += key_col(j) + " AS " + QId("_KEY_" + pk_names[j]) + ", ";
	}
	projection += "CAST(" + std::to_string(base_seq) + " + row_number() OVER () AS BIGINT) AS \"_SEQUENCE_NUMBER\", ";
	projection += "CAST(3 AS TINYINT) AS \"_VALUE_KIND\"";
	for (idx_t i = 0; i < value_names.size(); i++) {
		int j = key_index(value_names[i]);
		if (j >= 0) {
			projection += ", " + key_col(j) + " AS " + QId(value_names[i]);
		} else {
			projection += ", CAST(NULL AS " + value_types[i].ToString() + ") AS " + QId(value_names[i]);
		}
	}

	string uuid = UUID::ToString(UUID::GenerateRandomUUID());
	string data_file = bucket_dir + "/data-" + uuid + "-0.parquet";
	string copy_sql =
	    "COPY (SELECT " + projection + " FROM __paimon_del_keys) TO '" + data_file + "' (FORMAT PARQUET)";
	if (auto r = conn.Query(copy_sql); r && r->HasError()) {
		conn.Query("DROP TABLE __paimon_del_keys");
		throw IOException("Failed to write Paimon delete tombstones: " + r->GetError());
	}
	conn.Query("DROP TABLE __paimon_del_keys");

	PaimonWrittenFile wf;
	wf.file_path = data_file;
	wf.row_count = (int64_t)gstate.keys.Count();
	wf.bucket = 0;
	if (fs.FileExists(data_file)) {
		auto h = fs.OpenFile(data_file, FileFlags::FILE_FLAGS_READ);
		wf.file_size = h->GetFileSize();
	}
	vector<PaimonWrittenFile> written {wf};
	PaimonInsert::CommitWrittenFiles(context, table_path, written, gstate.keys.Count());
	return SinkFinalizeType::READY;
}

SourceResultType PaimonDelete::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<PaimonDeleteGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT((int64_t)gstate.delete_count));
	return SourceResultType::FINISHED;
}

string PaimonDelete::GetName() const {
	return "PAIMON_DELETE";
}

} // namespace duckdb
