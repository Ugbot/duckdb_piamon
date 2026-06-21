#include "paimon_functions.hpp"
#include "paimon_metadata.hpp"
#include "paimon_multi_file_reader.hpp"
#include "paimon_multi_file_list.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "storage/paimon_insert.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "iceberg_utils.hpp"

#include <unordered_map>
#include <utility>
#include <algorithm>

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
	fun.named_parameters["incremental_from"] = LogicalType::UBIGINT;
	fun.named_parameters["incremental_to"] = LogicalType::UBIGINT;
}

//===--------------------------------------------------------------------===//
// Paimon Scan Function
// Follows the Iceberg pattern: clone parquet_scan and inject PaimonMultiFileReader
//===--------------------------------------------------------------------===//

//===--------------------------------------------------------------------===//
// paimon_scan: reads the active data files of a Paimon table.
//
// Append-only tables are read directly. Primary-key tables are merged on read: the data files
// carry _SEQUENCE_NUMBER and _VALUE_KIND, and for the default `deduplicate` merge engine the row
// with the highest sequence number wins per key (delete tombstones drop the key). The merge is
// expressed as a read_parquet(...) query over the discovered files and executed on a *separate*
// connection at scan time — running it during bind re-enters the engine and deadlocks.
//===--------------------------------------------------------------------===//

// Paimon RowKind ordinals (org.apache.paimon.types.RowKind): INSERT=0, UPDATE_BEFORE=1,
// UPDATE_AFTER=2, DELETE=3. A row "exists" if its kind is an add (+I / +U).
static constexpr const char *VALUE_KIND_KEEP = "(0, 2)";

struct PaimonScanBindData : public TableFunctionData {
	string sql;            // the query to run (empty when there are no files)
	vector<LogicalType> return_types;
	vector<string> names;
	//! Output indices of the primary-key columns. The row-id virtual column emits these (a scalar
	//! for a single-column PK, a STRUCT for a composite PK) so catalog DELETE/UPDATE identify rows
	//! by key. Empty when there is no primary key.
	vector<idx_t> rowid_pk_indexes;
	//! Best-effort SQL predicate translated from the query's WHERE clause (set by
	//! pushdown_complex_filter). It is injected as an extra WHERE over the inner read_parquet query
	//! purely so the parquet reader can prune row groups; it is NOT relied on for correctness — the
	//! filter expressions are left in place above the scan, so DuckDB always re-applies the full
	//! predicate. Anything not safely translatable is simply omitted.
	mutable string pushdown_where;
};

struct PaimonScanGlobalState : public GlobalTableFunctionState {
	unique_ptr<Connection> conn;
	unique_ptr<QueryResult> result;
	unique_ptr<DataChunk> current; // keeps the fetched chunk alive while output references it
	vector<column_t> column_ids;   // projected columns (indices into the full schema)
};

static string QuoteIdentifier(const string &name) {
	string escaped;
	for (char c : name) {
		if (c == '"') {
			escaped += "\"\"";
		} else {
			escaped += c;
		}
	}
	return "\"" + escaped + "\"";
}

static string QuoteLiteral(const string &val) {
	string escaped;
	for (char c : val) {
		if (c == '\'') {
			escaped += "''";
		} else {
			escaped += c;
		}
	}
	return "'" + escaped + "'";
}

// SQL list literal of file paths: ['a', 'b', ...].
static string FileListLiteral(const vector<string> &files) {
	string s = "[";
	for (idx_t i = 0; i < files.size(); i++) {
		s += (i ? ", " : "") + QuoteLiteral(files[i]);
	}
	return s + "]";
}

// Build the SQL table source over the data files, dispatching on file format (by extension). Parquet
// is read with union_by_name so schema evolution (added/dropped/reordered columns) matches by name;
// Avro via read_avro (its reader has no union_by_name, but Paimon avro data files within a table share
// a schema). A table can mix formats across file.format changes, in which case the per-format reads
// are combined with UNION ALL BY NAME. ORC data files cannot be read — this DuckDB distribution has
// no ORC reader — so we fail with a clear, actionable error instead of misreading them.
static string BuildDataFileSource(const vector<string> &files) {
	vector<string> parquet, avro;
	for (auto &f : files) {
		auto dot = f.find_last_of('.');
		string ext = (dot == string::npos) ? "" : StringUtil::Lower(f.substr(dot + 1));
		if (ext == "avro") {
			avro.push_back(f);
		} else if (ext == "orc") {
			throw NotImplementedException(
			    "Paimon table uses ORC data files, which this DuckDB build cannot read (no ORC reader "
			    "is available). Only Parquet and Avro data files are supported — rewrite the table with "
			    "file.format=parquet, or read it via an engine with ORC support.");
		} else {
			// Parquet (Paimon's default) and any unknown/extensionless file fall through to read_parquet.
			parquet.push_back(f);
		}
	}
	string parquet_src =
	    parquet.empty() ? "" : "read_parquet(" + FileListLiteral(parquet) + ", union_by_name := true)";
	string avro_src = avro.empty() ? "" : "read_avro(" + FileListLiteral(avro) + ")";
	if (avro.empty()) {
		return parquet_src; // common path: parquet only
	}
	if (parquet.empty()) {
		return avro_src;
	}
	// Mixed parquet + avro: union the two readers, aligning columns by name.
	return "(SELECT * FROM " + parquet_src + " UNION ALL BY NAME SELECT * FROM " + avro_src + ")";
}

// ORDER BY clause for sequence ordering: the user-defined `sequence.field` columns (highest priority)
// followed by the physical `_pseq` (_SEQUENCE_NUMBER) tiebreaker, in the given direction.
static string SeqOrderClause(const vector<string> &seq_fields, const char *dir) {
	string s;
	for (auto &f : seq_fields) {
		s += QuoteIdentifier(f) + " " + dir + ", ";
	}
	s += "\"_pseq\" " + string(dir);
	return s;
}

// A single orderable value used by arg_max/arg_min (partial-update / first_value / last_value): a
// struct of the sequence.field columns + _pseq when a custom sequence is configured, else just _pseq.
static string SeqOrderValue(const vector<string> &seq_fields) {
	if (seq_fields.empty()) {
		return "\"_pseq\"";
	}
	string s = "row(";
	for (auto &f : seq_fields) {
		s += QuoteIdentifier(f) + ", ";
	}
	s += "\"_pseq\")";
	return s;
}

// SQL aggregate expression for the Paimon `aggregation` merge engine, mapping a Paimon
// fields.<col>.aggregate-function to its DuckDB equivalent over the per-key group. Columns with no
// configured function (or an unsupported one) fall back to last-non-null, matching Paimon's
// last_non_null_value behaviour for a sensible default.
static string AggExpr(const string &col, const string &fn, const string &order_val) {
	string q = QuoteIdentifier(col);
	if (fn == "sum") {
		return "sum(" + q + ")";
	}
	if (fn == "min") {
		return "min(" + q + ")";
	}
	if (fn == "max") {
		return "max(" + q + ")";
	}
	if (fn == "count") {
		return "count(" + q + ")";
	}
	if (fn == "product") {
		return "product(" + q + ")";
	}
	if (fn == "bool_and") {
		return "bool_and(" + q + ")";
	}
	if (fn == "bool_or") {
		return "bool_or(" + q + ")";
	}
	if (fn == "first_value") {
		return "arg_min(" + q + ", " + order_val + ")";
	}
	if (fn == "last_value") {
		return "arg_max(" + q + ", " + order_val + ")";
	}
	// last_non_null_value and default: latest value, ignoring nulls.
	return "arg_max(" + q + ", " + order_val + ") FILTER (WHERE " + q + " IS NOT NULL)";
}

static unique_ptr<FunctionData> PaimonScanBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<PaimonScanBindData>();
	string table_path = input.inputs[0].ToString();

	// Parse snapshot-selection named parameters for time travel.
	PaimonOptions options;
	for (auto &kv : input.named_parameters) {
		auto key = StringUtil::Lower(kv.first);
		if (key == "version") {
			options.table_version = StringValue::Get(kv.second);
		} else if (key == "snapshot_from_id") {
			options.snapshot_lookup.snapshot_source = PaimonOptions::SnapshotLookup::SnapshotSource::FROM_ID;
			options.snapshot_lookup.snapshot_id = kv.second.GetValue<uint64_t>();
		} else if (key == "snapshot_from_timestamp") {
			options.snapshot_lookup.snapshot_source = PaimonOptions::SnapshotLookup::SnapshotSource::FROM_TIMESTAMP;
			options.snapshot_lookup.snapshot_timestamp = kv.second.GetValue<timestamp_t>();
		} else if (key == "metadata_compression_codec") {
			options.metadata_compression_codec = StringValue::Get(kv.second);
		} else if (key == "incremental_to") {
			options.incremental = true;
			options.incremental_to = kv.second.GetValue<uint64_t>();
		} else if (key == "incremental_from") {
			options.incremental_from = kv.second.GetValue<uint64_t>();
		}
	}
	// incremental_to without incremental_from means "everything up to and including N" (from = 0).

	// Discover the active data files + load the schema (manifest-driven, with directory fallback).
	PaimonMultiFileList file_list(context, table_path, options);
	file_list.Bind(result->return_types, result->names, options);

	if (result->names.empty()) {
		throw IOException("Could not determine schema for Paimon table at: " + table_path);
	}

	// Determine merge semantics from the table schema. An incremental (changelog) read returns the
	// raw delta records of the snapshot range, so it is NOT merged even for primary-key tables.
	bool is_pk = !options.incremental && file_list.metadata && file_list.metadata->schema &&
	             !file_list.metadata->schema->primary_keys.empty();
	vector<string> primary_keys;
	string merge_engine = "deduplicate";
	vector<string> seq_fields; // sequence.field columns (custom merge ordering), if configured
	if (is_pk) {
		primary_keys = file_list.metadata->schema->primary_keys;
		auto &opts = file_list.metadata->schema->options;
		auto me = opts.find("merge-engine");
		if (me != opts.end()) {
			merge_engine = StringUtil::Lower(me->second);
		}
		auto sf = opts.find("sequence.field");
		if (sf != opts.end() && !sf->second.empty()) {
			seq_fields = StringUtil::Split(sf->second, ',');
			for (auto &f : seq_fields) {
				StringUtil::Trim(f);
			}
		}
	}

	// Record the output indices of the primary-key columns so the row-id virtual column can carry the
	// key (enabling catalog DELETE/UPDATE). Works for single- and multi-column keys of any type.
	for (auto &pk : primary_keys) {
		for (idx_t i = 0; i < result->names.size(); i++) {
			if (StringUtil::CIEquals(result->names[i], pk)) {
				result->rowid_pk_indexes.push_back(i);
				break;
			}
		}
	}

	return_types = result->return_types;
	names = result->names;

	// Field-id mapping across schema evolution: a column read from an older data file may have been
	// renamed, so we COALESCE over all historical names of each latest field id (latest name first).
	// Paimon field ids are stable; this matches the Spark/Flink readers (a renamed column keeps its
	// old data, unlike a name-only match). pk columns are immutable so this only affects values.
	case_insensitive_map_t<vector<string>> field_history; // latest column name -> [latest, ...older names]
	if (file_list.metadata && file_list.metadata->schema && file_list.metadata->schema->id > 0) {
		auto &latest = *file_list.metadata->schema;
		unordered_map<int, idx_t> id_to_pos; // field id -> index into result->names
		for (idx_t i = 0; i < latest.fields.size(); i++) {
			field_history[latest.fields[i].name] = {latest.fields[i].name};
			id_to_pos[latest.fields[i].id] = i;
		}
		FileSystem &fs2 = FileSystem::GetFileSystem(context);
		for (int64_t sid = latest.id - 1; sid >= 0; sid--) {
			try {
				auto old_schema = PaimonTableMetadata::LoadSchema(table_path, sid, fs2);
				if (!old_schema) {
					continue;
				}
				for (auto &of : old_schema->fields) {
					auto it = id_to_pos.find(of.id);
					if (it == id_to_pos.end()) {
						continue; // field dropped before the latest schema
					}
					auto &hist = field_history[latest.fields[it->second].name];
					if (std::find(hist.begin(), hist.end(), of.name) == hist.end()) {
						hist.push_back(of.name);
					}
				}
			} catch (const std::exception &e) {
				// Missing/unreadable older schema — skip.
			}
		}
	}

	// Build the projection list (cast each column to its Paimon logical type so the query result
	// types match the declared schema exactly).
	string projection;
	for (idx_t i = 0; i < result->names.size(); i++) {
		if (i > 0) {
			projection += ", ";
		}
		const string &col = result->names[i];
		string source_expr = QuoteIdentifier(col);
		auto hist = field_history.find(col);
		if (hist != field_history.end() && hist->second.size() > 1) {
			source_expr = "COALESCE(";
			for (idx_t h = 0; h < hist->second.size(); h++) {
				source_expr += (h ? ", " : "") + QuoteIdentifier(hist->second[h]);
			}
			source_expr += ")";
		}
		projection += "CAST(" + source_expr + " AS " + result->return_types[i].ToString() + ") AS " +
		              QuoteIdentifier(col);
	}

	if (file_list.files.empty()) {
		result->sql.clear(); // execute will return zero rows
		return std::move(result);
	}

	// Table source over the resolved active files, dispatched by file format (parquet/avro; ORC errors
	// out). union_by_name (parquet) handles schema evolution — files written under an older schema are
	// matched by column name and missing columns surface as NULL — and the outer projection then
	// selects the latest schema's columns in order.
	string file_array = BuildDataFileSource(file_list.files);

	if (!is_pk) {
		result->sql = "SELECT " + projection + " FROM " + file_array;
	} else {
		// Merge-on-read. Each PK data file carries _SEQUENCE_NUMBER and _VALUE_KIND. We first cast/
		// rename columns to the latest schema (`projection`) and expose the sequence + kind as _pseq /
		// _pvk, then apply the table's configured merge engine per primary key.
		string pk_cols;
		for (idx_t i = 0; i < primary_keys.size(); i++) {
			pk_cols += (i ? ", " : "") + QuoteIdentifier(primary_keys[i]);
		}
		string base = "SELECT " + projection + ", \"_SEQUENCE_NUMBER\" AS _pseq, \"_VALUE_KIND\" AS _pvk FROM " +
		              file_array;
		// `base` already casts/renames to the latest schema, so downstream selects reference the final
		// column names directly (not the COALESCE projection, whose source columns no longer exist).
		string name_list;
		for (idx_t i = 0; i < result->names.size(); i++) {
			name_list += (i ? ", " : "") + QuoteIdentifier(result->names[i]);
		}

		if (merge_engine == "first-row") {
			// Keep the FIRST row per key (lowest sequence); later rows for the key are ignored.
			string inner = "SELECT *, row_number() OVER (PARTITION BY " + pk_cols + " ORDER BY " +
			               SeqOrderClause(seq_fields, "ASC") + ") AS _paimon_rn FROM (" + base + ")";
			result->sql = "SELECT " + name_list + " FROM (" + inner + ") WHERE _paimon_rn = 1 AND _pvk IN " +
			              string(VALUE_KIND_KEEP);
		} else if (merge_engine == "partial-update" || merge_engine == "aggregation") {
			// Collapse each key to one row: partial-update takes the latest non-null value per column;
			// aggregation applies each column's configured aggregate function. Delete records (_pvk=3)
			// are excluded (retraction/sequence-group semantics are not modelled here).
			bool is_agg = (merge_engine == "aggregation");
			string order_val = SeqOrderValue(seq_fields);
			auto &opts = file_list.metadata->schema->options;
			case_insensitive_set_t pk_set(primary_keys.begin(), primary_keys.end());
			string select_list;
			for (idx_t i = 0; i < result->names.size(); i++) {
				const string &col = result->names[i];
				select_list += (i ? ", " : "");
				if (pk_set.find(col) != pk_set.end()) {
					select_list += QuoteIdentifier(col);
					continue;
				}
				string expr;
				if (is_agg) {
					string fn;
					auto fit = opts.find("fields." + col + ".aggregate-function");
					if (fit != opts.end()) {
						fn = StringUtil::Lower(fit->second);
					}
					expr = AggExpr(col, fn, order_val);
				} else {
					expr = "arg_max(" + QuoteIdentifier(col) + ", " + order_val + ") FILTER (WHERE " +
					       QuoteIdentifier(col) + " IS NOT NULL)";
				}
				// Aggregates can widen the type (e.g. sum(BIGINT)->HUGEINT); cast back to the declared
				// schema type so the result columns match the table's logical types exactly.
				select_list += "CAST(" + expr + " AS " + result->return_types[i].ToString() + ") AS " +
				               QuoteIdentifier(col);
			}
			result->sql = "SELECT " + select_list + " FROM (" + base + ") WHERE _pvk <> 3 GROUP BY " + pk_cols;
		} else {
			// deduplicate (default): keep the highest-sequence row per key, dropping tombstones.
			string inner = "SELECT *, row_number() OVER (PARTITION BY " + pk_cols + " ORDER BY " +
			               SeqOrderClause(seq_fields, "DESC") + ") AS _paimon_rn FROM (" + base + ")";
			result->sql = "SELECT " + name_list + " FROM (" + inner + ") WHERE _paimon_rn = 1 AND _pvk IN " +
			              string(VALUE_KIND_KEEP);
		}
	}

	return std::move(result);
}

// SQL operator symbol for a binary comparison; nullptr for anything else.
static const char *ComparisonOpSql(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return " = ";
	case ExpressionType::COMPARE_NOTEQUAL:
		return " <> ";
	case ExpressionType::COMPARE_LESSTHAN:
		return " < ";
	case ExpressionType::COMPARE_GREATERTHAN:
		return " > ";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return " <= ";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return " >= ";
	default:
		return nullptr;
	}
}

// Recursively translate a bound filter expression into a SQL predicate string, returning "" when it
// (or a required sub-part) cannot be exactly represented. The result is only used to prune the inner
// parquet scan; the original predicate stays above the scan so correctness never depends on this.
// Over-approximation is therefore safe for AND (drop untranslatable conjuncts → wider result), but an
// OR must be abandoned entirely if any branch is untranslatable (a partial OR would wrongly drop rows).
static string TranslateFilterExpr(const Expression &expr, const LogicalGet &get,
                                  const case_insensitive_set_t &valid_names) {
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &col = expr.Cast<BoundColumnRefExpression>();
		// binding.column_index indexes into the get's projected column list, not the full schema:
		// resolve it through GetColumnIds() to the schema position, then to the column name. A
		// virtual/row-id column has a primary index past the real columns and is skipped.
		auto idx = col.binding.column_index;
		auto &col_ids = get.GetColumnIds();
		if (idx >= col_ids.size()) {
			return "";
		}
		auto prim = col_ids[idx].GetPrimaryIndex();
		if (prim >= get.names.size()) {
			return "";
		}
		const string &name = get.names[prim];
		if (valid_names.find(name) == valid_names.end()) {
			return "";
		}
		return QuoteIdentifier(name);
	}
	case ExpressionClass::BOUND_CONSTANT: {
		auto &c = expr.Cast<BoundConstantExpression>();
		if (c.value.IsNull()) {
			return ""; // NULL comparisons are three-valued; leave them to DuckDB
		}
		return c.value.ToSQLString();
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &cmp = expr.Cast<BoundComparisonExpression>();
		auto op = ComparisonOpSql(cmp.type);
		if (!op) {
			return "";
		}
		string l = TranslateFilterExpr(*cmp.left, get, valid_names);
		string r = TranslateFilterExpr(*cmp.right, get, valid_names);
		if (l.empty() || r.empty()) {
			return "";
		}
		return "(" + l + op + r + ")";
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		if (expr.type == ExpressionType::OPERATOR_IS_NULL || expr.type == ExpressionType::OPERATOR_IS_NOT_NULL) {
			if (op.children.size() != 1) {
				return "";
			}
			string c = TranslateFilterExpr(*op.children[0], get, valid_names);
			if (c.empty()) {
				return "";
			}
			return "(" + c + (expr.type == ExpressionType::OPERATOR_IS_NULL ? " IS NULL)" : " IS NOT NULL)");
		}
		if (expr.type == ExpressionType::COMPARE_IN || expr.type == ExpressionType::COMPARE_NOT_IN) {
			if (op.children.size() < 2) {
				return "";
			}
			string col = TranslateFilterExpr(*op.children[0], get, valid_names);
			if (col.empty()) {
				return "";
			}
			string list;
			for (idx_t i = 1; i < op.children.size(); i++) {
				if (op.children[i]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
					return ""; // only constant IN-lists translate cleanly
				}
				auto &cv = op.children[i]->Cast<BoundConstantExpression>();
				if (cv.value.IsNull()) {
					return "";
				}
				list += (i > 1 ? ", " : "") + cv.value.ToSQLString();
			}
			return "(" + col + (expr.type == ExpressionType::COMPARE_IN ? " IN (" : " NOT IN (") + list + "))";
		}
		return "";
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		bool is_and = (expr.type == ExpressionType::CONJUNCTION_AND);
		vector<string> parts;
		for (auto &child : conj.children) {
			string c = TranslateFilterExpr(*child, get, valid_names);
			if (c.empty()) {
				if (is_and) {
					continue; // dropping an AND conjunct only widens the result — safe
				}
				return ""; // an OR with an untranslatable branch cannot be partially applied
			}
			parts.push_back(std::move(c));
		}
		if (parts.empty()) {
			return "";
		}
		if (parts.size() == 1) {
			return parts[0];
		}
		string joined = "(";
		for (idx_t i = 0; i < parts.size(); i++) {
			joined += (i ? (is_and ? " AND " : " OR ") : "") + parts[i];
		}
		joined += ")";
		return joined;
	}
	default:
		return "";
	}
}

// Predicate pushdown: translate the query's WHERE conjuncts into a SQL predicate that is injected over
// the inner read_parquet query so the parquet reader can skip row groups. The filters are deliberately
// left in `filters` (not consumed), so DuckDB re-applies the full predicate above the scan and this
// remains a pure performance hint — anything we cannot translate exactly is simply skipped.
static void PaimonScanComplexFilterPushdown(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                            vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<PaimonScanBindData>();
	if (filters.empty() || bind_data.sql.empty()) {
		return;
	}
	case_insensitive_set_t valid_names(bind_data.names.begin(), bind_data.names.end());
	vector<string> terms;
	for (auto &f : filters) {
		string t = TranslateFilterExpr(*f, get, valid_names);
		if (!t.empty()) {
			terms.push_back(std::move(t));
		}
	}
	if (terms.empty()) {
		return;
	}
	string where;
	for (idx_t i = 0; i < terms.size(); i++) {
		where += (i ? " AND " : "") + terms[i];
	}
	bind_data.pushdown_where = std::move(where);
}

static unique_ptr<GlobalTableFunctionState> PaimonScanInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<PaimonScanBindData>();
	auto state = make_uniq<PaimonScanGlobalState>();
	state->column_ids = input.column_ids;
	if (bind_data.sql.empty()) {
		return std::move(state); // no files → no rows
	}
	// Avro data files are read via read_avro (the avro extension). Paimon does not force-load avro at
	// startup (the common parquet path needs no avro), so load it lazily only when avro files are
	// actually present. TryAutoLoadExtension makes this transparent where autoloading is enabled and
	// is a no-op when avro is already loaded; otherwise read_avro's own error (INSTALL avro; LOAD
	// avro;) surfaces from the query below.
	if (bind_data.sql.find("read_avro(") != string::npos) {
		ExtensionHelper::TryAutoLoadExtension(context, "avro");
	}
	// Execute on a fresh connection so we don't re-enter the binding context. If a predicate was
	// pushed down, wrap the query so the parquet reader prunes row groups (a pure optimization; the
	// full filter is still applied by the calling query).
	string query = bind_data.sql;
	if (!bind_data.pushdown_where.empty()) {
		query = "SELECT * FROM (" + query + ") AS _paimon_pushdown WHERE " + bind_data.pushdown_where;
	}
	state->conn = make_uniq<Connection>(DatabaseInstance::GetDatabase(context));
	auto pending = state->conn->Query(query);
	if (pending->HasError()) {
		throw IOException("Paimon scan failed: " + pending->GetError());
	}
	state->result = std::move(pending);
	return std::move(state);
}

static void PaimonScanExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<PaimonScanGlobalState>();
	auto &bind_data = data.bind_data->Cast<PaimonScanBindData>();
	if (!state.result) {
		output.SetCardinality(0);
		return;
	}
	state.current = state.result->Fetch();
	if (!state.current || state.current->size() == 0) {
		output.SetCardinality(0);
		return;
	}
	auto &chunk = *state.current;
	// Apply projection pushdown: output the requested columns (column_ids index into the full
	// schema). The fetched chunk is retained in the global state so these references stay valid.
	for (idx_t i = 0; i < state.column_ids.size(); i++) {
		auto col_id = state.column_ids[i];
		if (col_id >= chunk.ColumnCount()) {
			// Row-id / virtual column. Emit the primary key so catalog DELETE/UPDATE can identify
			// rows — driven by the requested output type: a STRUCT for a composite key, a scalar when
			// the requested type matches the single key column, otherwise a row-number sequence (the
			// catalog-free `paimon_scan('path')` rowid is plain BIGINT; only its cardinality matters).
			auto &out_type = output.data[i].GetType();
			auto &pk_idx = bind_data.rowid_pk_indexes;
			if (out_type.id() == LogicalTypeId::STRUCT && StructType::GetChildCount(out_type) == pk_idx.size() &&
			    !pk_idx.empty()) {
				auto &entries = StructVector::GetEntries(output.data[i]);
				for (idx_t j = 0; j < pk_idx.size(); j++) {
					entries[j]->Reference(chunk.data[pk_idx[j]]);
				}
			} else if (pk_idx.size() == 1 && out_type == chunk.data[pk_idx[0]].GetType()) {
				output.data[i].Reference(chunk.data[pk_idx[0]]);
			} else {
				output.data[i].Sequence(0, 1, chunk.size());
			}
		} else {
			output.data[i].Reference(chunk.data[col_id]);
		}
	}
	output.SetCardinality(chunk.size());
}

TableFunctionSet PaimonFunctions::GetPaimonScanFunction(ExtensionLoader &loader) {
	TableFunctionSet set("paimon_scan");
	TableFunction scan({LogicalType::VARCHAR}, PaimonScanExec, PaimonScanBind, PaimonScanInitGlobal);
	scan.name = "paimon_scan";
	scan.projection_pushdown = true;
	scan.pushdown_complex_filter = PaimonScanComplexFilterPushdown;
	AddPaimonNamedParameters(scan);
	set.AddFunction(scan);
	return set;
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
// paimon_compact: rewrite a table's active data files into a single file and commit a COMPACT
// snapshot that references only the new file (reduces read amplification; merge-on-read still
// resolves the logical content). Returns the number of files that were compacted.
//===--------------------------------------------------------------------===//

struct PaimonCompactBindData : public TableFunctionData {
	string table_path;
};

struct PaimonCompactGlobalState : public GlobalTableFunctionState {
	int64_t files_compacted = 0;
	bool emitted = false;
};

static unique_ptr<FunctionData> PaimonCompactBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<PaimonCompactBindData>();
	result->table_path = input.inputs[0].ToString();
	return_types.push_back(LogicalType::BIGINT);
	names.push_back("files_compacted");
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> PaimonCompactInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<PaimonCompactBindData>();
	auto state = make_uniq<PaimonCompactGlobalState>();
	const string &table_path = bind_data.table_path;

	// Discover the current active files + schema.
	PaimonMultiFileList file_list(context, table_path);
	PaimonOptions options;
	vector<LogicalType> types;
	vector<string> value_names;
	file_list.Bind(types, value_names, options);
	if (value_names.empty() || file_list.files.size() <= 1) {
		// Nothing worth compacting (no schema, or already a single file).
		state->files_compacted = 0;
		return std::move(state);
	}
	bool is_pk =
	    file_list.metadata && file_list.metadata->schema && !file_list.metadata->schema->primary_keys.empty();

	Connection conn(DatabaseInstance::GetDatabase(context));
	FileSystem &fs = FileSystem::GetFileSystem(context);
	string bucket_dir = table_path + "/bucket-0";
	if (!fs.DirectoryExists(bucket_dir)) {
		fs.CreateDirectory(bucket_dir);
	}

	string proj;
	for (idx_t i = 0; i < value_names.size(); i++) {
		proj += (i ? ", " : "") + QuoteIdentifier(value_names[i]);
	}
	string select_merged;
	if (is_pk) {
		// Rewrite the merged (deduplicated) rows with fresh sequence numbers and INSERT kind.
		string sys;
		for (auto &pk : file_list.metadata->schema->primary_keys) {
			sys += QuoteIdentifier(pk) + " AS " + QuoteIdentifier("_KEY_" + pk) + ", ";
		}
		sys += "CAST(row_number() OVER () AS BIGINT) AS \"_SEQUENCE_NUMBER\", CAST(0 AS TINYINT) AS \"_VALUE_KIND\"";
		select_merged = "SELECT " + sys + ", " + proj + " FROM paimon_scan(" + QuoteLiteral(table_path) + ")";
	} else {
		select_merged = "SELECT " + proj + " FROM paimon_scan(" + QuoteLiteral(table_path) + ")";
	}

	string uuid = UUID::ToString(UUID::GenerateRandomUUID());
	string data_file = bucket_dir + "/data-" + uuid + "-0.parquet";
	auto copy_res =
	    conn.Query("COPY (" + select_merged + ") TO '" + data_file + "' (FORMAT PARQUET)");
	if (copy_res->HasError()) {
		throw IOException("Paimon compaction failed writing merged file: " + copy_res->GetError());
	}

	int64_t row_count = 0;
	{
		auto r = conn.Query("SELECT count(*) FROM read_parquet('" + data_file + "')");
		if (r && !r->HasError()) {
			auto v = r->Fetch();
			if (v && v->size() > 0) {
				row_count = v->GetValue(0, 0).GetValue<int64_t>();
			}
		}
	}

	PaimonWrittenFile wf;
	wf.file_path = data_file;
	wf.row_count = row_count;
	wf.bucket = 0;
	if (fs.FileExists(data_file)) {
		auto h = fs.OpenFile(data_file, FileFlags::FILE_FLAGS_READ);
		wf.file_size = h->GetFileSize();
	}
	state->files_compacted = (int64_t)file_list.files.size();
	// Replace the active set with just the merged file (no carry-forward), as a COMPACT snapshot.
	PaimonInsert::CommitWrittenFiles(context, table_path, {wf}, (idx_t)row_count, false, "COMPACT");
	return std::move(state);
}

static void PaimonCompactExec(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<PaimonCompactGlobalState>();
	if (state.emitted) {
		output.SetCardinality(0);
		return;
	}
	state.emitted = true;
	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BIGINT(state.files_compacted));
}

static TableFunctionSet GetPaimonCompactFunction() {
	TableFunctionSet set("paimon_compact");
	TableFunction f({LogicalType::VARCHAR}, PaimonCompactExec, PaimonCompactBind, PaimonCompactInitGlobal);
	f.name = "paimon_compact";
	set.AddFunction(f);
	return set;
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
	functions.push_back(GetPaimonCompactFunction());

	return functions;
}

// Simple test function implementation
void PaimonFunctions::PaimonTestFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetValue(0, Value("Paimon extension is loaded!"));
}

} // namespace duckdb
