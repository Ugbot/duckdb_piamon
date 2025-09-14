#include "storage/paimon_update.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/delete_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/update_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "storage/paimon_table_entry.hpp"

namespace duckdb {

PaimonUpdate::PaimonUpdate(PhysicalPlan &physical_plan, vector<LogicalType> types, TableCatalogEntry &tableref,
                           vector<PhysicalIndex> columns, vector<unique_ptr<Expression>> expressions,
                           vector<unique_ptr<Expression>> bound_defaults,
                           vector<unique_ptr<BoundConstraint>> bound_constraints, idx_t estimated_cardinality,
                           bool return_chunk)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::UPDATE, std::move(types), estimated_cardinality),
      tableref(tableref), columns(std::move(columns)), expressions(std::move(expressions)),
      bound_defaults(std::move(bound_defaults)), bound_constraints(std::move(bound_constraints)),
      return_chunk(return_chunk) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class PaimonUpdateGlobalState : public GlobalSinkState {
public:
	explicit PaimonUpdateGlobalState() = default;

	//! The amount of rows updated
	idx_t update_count = 0;
	//! ColumnDataCollection to store the result of the update
	ColumnDataCollection return_collection;
};

class PaimonUpdateLocalState : public LocalSinkState {
public:
	explicit PaimonUpdateLocalState(ClientContext &context, const vector<LogicalType> &types) {
		// Initialize local state for update processing
	}
};

//===--------------------------------------------------------------------===//
// Getters
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSinkState> PaimonUpdate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<PaimonUpdateGlobalState>();
}

unique_ptr<LocalSinkState> PaimonUpdate::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<PaimonUpdateLocalState>(context.client, types);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PaimonUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state->Cast<PaimonUpdateGlobalState>();
	auto &local_state = input.local_state->Cast<PaimonUpdateLocalState>();

	// For now, this is a placeholder implementation
	// In a full implementation, this would:
	// 1. Read the existing data that matches the WHERE clause
	// 2. Apply the UPDATE expressions to modify the data
	// 3. Write new data files with the updated records
	// 4. Update Paimon metadata with new snapshot

	global_state.update_count += chunk.size();

	if (return_chunk) {
		global_state.return_collection.Append(chunk);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType PaimonUpdate::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state->Cast<PaimonUpdateGlobalState>();
	auto &local_state = input.local_state->Cast<PaimonUpdateLocalState>();

	// Combine results from parallel workers
	// For now, just return success

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PaimonUpdateSourceState : public GlobalSourceState {
public:
	explicit PaimonUpdateSourceState(const PaimonUpdate &op) {
	}

public:
	bool finished = false;
};

unique_ptr<GlobalSourceState> PaimonUpdate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PaimonUpdateSourceState>(*this);
}

SourceResultType PaimonUpdate::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &state = input.global_state->Cast<PaimonUpdateSourceState>();

	if (finished) {
		return SourceResultType::FINISHED;
	}

	auto &global_state = sink_state->Cast<PaimonUpdateGlobalState>();

	if (return_chunk) {
		global_state.return_collection.Reset();
		DataChunk return_chunk;
		global_state.return_collection.FetchChunk(0, return_chunk);
		chunk.Reference(return_chunk);
	}

	finished = true;
	return SourceResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string PaimonUpdate::GetName() const {
	return "PAIMON_UPDATE";
}

InsertionOrderPreservingMap<string> PaimonUpdate::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = tableref.name;
	string columns_str;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			columns_str += ", ";
		}
		columns_str += std::to_string(columns[i].index);
	}
	result["Columns"] = columns_str;
	return result;
}

} // namespace duckdb
