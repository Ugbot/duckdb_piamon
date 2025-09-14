#include "storage/paimon_delete.hpp"
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

PaimonDelete::PaimonDelete(PhysicalPlan &physical_plan, vector<LogicalType> types, TableCatalogEntry &tableref,
                           vector<unique_ptr<Expression>> expressions,
                           vector<unique_ptr<BoundConstraint>> bound_constraints, idx_t estimated_cardinality,
                           bool return_chunk)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::DELETE, std::move(types), estimated_cardinality),
      tableref(tableref), expressions(std::move(expressions)), bound_constraints(std::move(bound_constraints)),
      return_chunk(return_chunk) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class PaimonDeleteGlobalState : public GlobalSinkState {
public:
	explicit PaimonDeleteGlobalState() = default;

	//! The amount of rows deleted
	idx_t delete_count = 0;
	//! ColumnDataCollection to store the result of the delete
	ColumnDataCollection return_collection;
};

class PaimonDeleteLocalState : public LocalSinkState {
public:
	explicit PaimonDeleteLocalState(ClientContext &context, const vector<LogicalType> &types) {
		// Initialize local state for delete processing
	}
};

//===--------------------------------------------------------------------===//
// Getters
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSinkState> PaimonDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<PaimonDeleteGlobalState>();
}

unique_ptr<LocalSinkState> PaimonDelete::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<PaimonDeleteLocalState>(context.client, types);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PaimonDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &global_state = input.global_state->Cast<PaimonDeleteGlobalState>();
	auto &local_state = input.local_state->Cast<PaimonDeleteLocalState>();

	// For now, this is a placeholder implementation
	// In a full implementation, this would:
	// 1. Read the existing data that matches the WHERE clause
	// 2. Create delete files (equality deletes or position deletes)
	// 3. Update Paimon metadata with new snapshot

	global_state.delete_count += chunk.size();

	if (return_chunk) {
		global_state.return_collection.Append(chunk);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType PaimonDelete::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state->Cast<PaimonDeleteGlobalState>();
	auto &local_state = input.local_state->Cast<PaimonDeleteLocalState>();

	// Combine results from parallel workers
	// For now, just return success

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PaimonDeleteSourceState : public GlobalSourceState {
public:
	explicit PaimonDeleteSourceState(const PaimonDelete &op) {
	}

public:
	bool finished = false;
};

unique_ptr<GlobalSourceState> PaimonDelete::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PaimonDeleteSourceState>(*this);
}

SourceResultType PaimonDelete::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &state = input.global_state->Cast<PaimonDeleteSourceState>();

	if (finished) {
		return SourceResultType::FINISHED;
	}

	auto &global_state = sink_state->Cast<PaimonDeleteGlobalState>();

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
string PaimonDelete::GetName() const {
	return "PAIMON_DELETE";
}

InsertionOrderPreservingMap<string> PaimonDelete::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table"] = tableref.name;
	return result;
}

} // namespace duckdb
