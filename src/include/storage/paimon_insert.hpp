//===----------------------------------------------------------------------===//
//                         DuckDB
//
// paimon_insert.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/execution/physical_operator_states.hpp"

namespace duckdb {

struct PaimonInsertGlobalState;

class PaimonInsert : public PhysicalOperator {
public:
	PaimonInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
	            physical_index_vector_t<idx_t> column_index_map_p);

	PaimonInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema);

public:
	// PhysicalOperator interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const;

	// Sink interface
	bool IsSink() const override;
	bool ParallelSink() const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                         OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	PhysicalOperatorType GetOperatorType() const override {
		return PhysicalOperatorType::EXTENSION;
	}

private:
	// Helper methods
	void WriteChunkToParquet(ClientContext &context, DataChunk &chunk, const string &file_path) const;
	void UpdatePaimonMetadata(ClientContext &context, const struct PaimonInsertGlobalState &global_state) const;

public:
	TableCatalogEntry *table;
	SchemaCatalogEntry *schema;
	physical_index_vector_t<idx_t> column_index_map;
};

struct PaimonCopyInput {
	PaimonCopyInput(ClientContext &context, TableCatalogEntry &table);
	PaimonCopyInput(ClientContext &context, SchemaCatalogEntry &schema, const ColumnList &columns,
	               const string &data_path_p);
};

} // namespace duckdb
