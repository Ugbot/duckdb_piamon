//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/paimon_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {

//! PhysicalUpdate represents an UPDATE operation
class PaimonUpdate : public PhysicalOperator {
public:
	PaimonUpdate(PhysicalPlan &physical_plan, vector<LogicalType> types, TableCatalogEntry &tableref,
	              vector<PhysicalIndex> columns, vector<unique_ptr<Expression>> expressions,
	              vector<unique_ptr<Expression>> bound_defaults,
	              vector<unique_ptr<BoundConstraint>> bound_constraints, idx_t estimated_cardinality,
	              bool return_chunk);

public:
	//! The table to update
	TableCatalogEntry &tableref;
	//! The columns to update
	vector<PhysicalIndex> columns;
	//! The expressions to update with
	vector<unique_ptr<Expression>> expressions;
	//! The bound defaults
	vector<unique_ptr<Expression>> bound_defaults;
	//! The bound constraints
	vector<unique_ptr<BoundConstraint>> bound_constraints;
	//! Whether to return the updated chunk
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
