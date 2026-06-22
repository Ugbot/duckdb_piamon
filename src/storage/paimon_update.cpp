#include "storage/paimon_update.hpp"
#include "storage/paimon_insert.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/appender.hpp"

namespace duckdb {

PaimonUpdate::PaimonUpdate(PhysicalPlan &physical_plan, vector<LogicalType> types, TableCatalogEntry &table,
                           idx_t row_id_index, string table_path, vector<string> pk_names, vector<string> value_names,
                           vector<LogicalType> value_types, vector<string> updated_columns,
                           vector<idx_t> updated_child_indexes, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      table(table), row_id_index(row_id_index), table_path(std::move(table_path)), pk_names(std::move(pk_names)),
      value_names(std::move(value_names)), value_types(std::move(value_types)),
      updated_columns(std::move(updated_columns)), updated_child_indexes(std::move(updated_child_indexes)) {
}

// Types of the primary-key columns, in pk order.
static vector<LogicalType> UpdateKeyTypes(const vector<string> &pk_names, const vector<string> &value_names,
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

static string QId(const string &n) {
	string e;
	for (char c : n) {
		e += (c == '"') ? "\"\"" : string(1, c);
	}
	return "\"" + e + "\"";
}

class PaimonUpdateGlobalState : public GlobalSinkState {
public:
	PaimonUpdateGlobalState(ClientContext &context, const vector<LogicalType> &buffer_types)
	    : buffered(context, buffer_types), update_count(0) {
	}
	ColumnDataCollection buffered; //! [key_0..key_{K-1}, updated_col_0, updated_col_1, ...]
	idx_t update_count;
};

unique_ptr<GlobalSinkState> PaimonUpdate::GetGlobalSinkState(ClientContext &context) const {
	vector<LogicalType> buffer_types = UpdateKeyTypes(pk_names, value_names, value_types); // key columns
	for (auto &uc : updated_columns) {
		for (idx_t i = 0; i < value_names.size(); i++) {
			if (StringUtil::CIEquals(value_names[i], uc)) {
				buffer_types.push_back(value_types[i]);
				break;
			}
		}
	}
	return make_uniq<PaimonUpdateGlobalState>(context, buffer_types);
}

SinkResultType PaimonUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<PaimonUpdateGlobalState>();
	idx_t nkeys = pk_names.size();
	vector<LogicalType> buffer_types = UpdateKeyTypes(pk_names, value_names, value_types);
	for (auto idx : updated_child_indexes) {
		buffer_types.push_back(chunk.data[idx].GetType());
	}
	DataChunk buf;
	buf.InitializeEmpty(buffer_types);
	// The row id (last child column) carries the key: scalar for single PK, STRUCT for composite.
	auto &rid = chunk.data[chunk.ColumnCount() - 1];
	if (nkeys == 1) {
		buf.data[0].Reference(rid);
	} else {
		auto &entries = StructVector::GetEntries(rid);
		for (idx_t j = 0; j < nkeys; j++) {
			buf.data[j].Reference(*entries[j]);
		}
	}
	for (idx_t j = 0; j < updated_child_indexes.size(); j++) {
		buf.data[nkeys + j].Reference(chunk.data[updated_child_indexes[j]]);
	}
	buf.SetCardinality(chunk.size());
	gstate.buffered.Append(buf);
	gstate.update_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PaimonUpdate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                        OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<PaimonUpdateGlobalState>();
	if (gstate.buffered.Count() == 0) {
		return SinkFinalizeType::READY;
	}
	FileSystem &fs = FileSystem::GetFileSystem(context);
	Connection conn(DatabaseInstance::GetDatabase(context));
	string bucket_dir = table_path + "/bucket-0";
	if (!fs.DirectoryExists(bucket_dir)) {
		fs.CreateDirectory(bucket_dir);
	}

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

	// Stage buffered (key cols, updated values) into a temp table: k0..k{K-1}, u0, u1, ...
	auto key_types = UpdateKeyTypes(pk_names, value_names, value_types);
	auto key_col = [&](idx_t j) { return "k" + std::to_string(j); };
	string stage_cols;
	for (idx_t j = 0; j < pk_names.size(); j++) {
		stage_cols += (j ? ", " : "") + key_col(j) + " " + key_types[j].ToString();
	}
	for (idx_t j = 0; j < updated_columns.size(); j++) {
		string t = "VARCHAR";
		for (idx_t i = 0; i < value_names.size(); i++) {
			if (StringUtil::CIEquals(value_names[i], updated_columns[j])) {
				t = value_types[i].ToString();
				break;
			}
		}
		stage_cols += ", u" + std::to_string(j) + " " + t;
	}
	if (auto r = conn.Query("CREATE TEMP TABLE __paimon_upd (" + stage_cols + ")"); r && r->HasError()) {
		throw IOException("Failed to stage Paimon update rows: " + r->GetError());
	}
	{
		Appender appender(conn, "__paimon_upd");
		for (auto &chunk : gstate.buffered.Chunks()) {
			appender.AppendDataChunk(const_cast<DataChunk &>(chunk));
		}
		appender.Close();
	}

	auto pk_index = [&](const string &col) -> int {
		for (idx_t j = 0; j < pk_names.size(); j++) {
			if (StringUtil::CIEquals(pk_names[j], col)) {
				return (int)j;
			}
		}
		return -1;
	};
	auto updated_index = [&](const string &col) -> int {
		for (idx_t j = 0; j < updated_columns.size(); j++) {
			if (StringUtil::CIEquals(updated_columns[j], col)) {
				return (int)j;
			}
		}
		return -1;
	};
	// New full row per matched key: current row (via paimon_scan), with SET columns overwritten.
	string projection;
	for (idx_t j = 0; j < pk_names.size(); j++) {
		projection += "c." + QId(pk_names[j]) + " AS " + QId("_KEY_" + pk_names[j]) + ", ";
	}
	projection += "CAST(" + std::to_string(base_seq) + " + row_number() OVER () AS BIGINT) AS \"_SEQUENCE_NUMBER\", ";
	projection += "CAST(0 AS TINYINT) AS \"_VALUE_KIND\"";
	for (auto &vn : value_names) {
		int j = updated_index(vn);
		if (j >= 0) {
			projection += ", u.u" + std::to_string(j) + " AS " + QId(vn);
		} else {
			projection += ", c." + QId(vn) + " AS " + QId(vn);
		}
	}

	string join_cond;
	for (idx_t j = 0; j < pk_names.size(); j++) {
		join_cond += (j ? " AND " : "") + ("c." + QId(pk_names[j]) + " = u." + key_col(j));
	}
	string uuid = UUID::ToString(UUID::GenerateRandomUUID());
	string data_file = bucket_dir + "/data-" + uuid + "-0.parquet";
	string copy_sql = "COPY (SELECT " + projection + " FROM paimon_scan('" + table_path + "') c JOIN __paimon_upd u ON " +
	                  join_cond + ") TO '" + data_file + "' (FORMAT PARQUET)";
	if (auto r = conn.Query(copy_sql); r && r->HasError()) {
		conn.Query("DROP TABLE __paimon_upd");
		throw IOException("Failed to write Paimon update rows: " + r->GetError());
	}
	conn.Query("DROP TABLE __paimon_upd");

	PaimonWrittenFile wf;
	wf.file_path = data_file;
	wf.row_count = (int64_t)gstate.update_count;
	wf.bucket = 0;
	if (!fs.FileExists(data_file)) {
		throw IOException("Paimon UPDATE: data file missing after COPY: " + data_file);
	}
	{
		auto h = fs.OpenFile(data_file, FileFlags::FILE_FLAGS_READ);
		wf.file_size = h->GetFileSize();
	}
	vector<PaimonWrittenFile> written {wf};
	PaimonInsert::CommitWrittenFiles(context, table_path, written, gstate.update_count);
	return SinkFinalizeType::READY;
}

SourceResultType PaimonUpdate::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<PaimonUpdateGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT((int64_t)gstate.update_count));
	return SourceResultType::FINISHED;
}

string PaimonUpdate::GetName() const {
	return "PAIMON_UPDATE";
}

} // namespace duckdb
