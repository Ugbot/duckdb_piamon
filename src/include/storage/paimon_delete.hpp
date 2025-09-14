//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/paimon_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {

//! PhysicalDelete represents a DELETE operation
class PaimonDelete : public PhysicalOperator {
public:
	PaimonDelete(PhysicalPlan &physical_plan, vector<LogicalType> types, TableCatalogEntry &tableref,
	              vector<unique_ptr<Expression>> expressions,
	              vector<unique_ptr<BoundConstraint>> bound_constraints, idx_t estimated_cardinality,
	              bool return_chunk);

public:
	//! The table to delete from
	TableCatalogEntry &tableref;
	//! The expressions for the WHERE clause
	vector<unique_ptr<Expression>> expressions;
	//! The bound constraints
	vector<unique_ptr<BoundConstraint>> bound_constraints;
	//! Whether to return the deleted chunk
	bool return_chunk;

public:
	// Source interface
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;

	// State interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

	// Operator interface
	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
	mutable bool finished = false;
};

} // namespace duckdb
