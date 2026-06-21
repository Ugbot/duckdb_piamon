#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

//! Physical DELETE for primary-key Paimon tables. The child plan produces the deleted rows; the
//! row-id column (= the primary-key value, see paimon_scan) is buffered and, on Finalize, written
//! as a data file of delete tombstones (_VALUE_KIND = DELETE) at a higher sequence number than any
//! existing row, so merge-on-read drops those keys.
class PaimonDelete : public PhysicalOperator {
public:
	PaimonDelete(PhysicalPlan &physical_plan, vector<LogicalType> types, TableCatalogEntry &table, idx_t row_id_index,
	             string table_path, vector<string> pk_names, vector<string> value_names,
	             vector<LogicalType> value_types, idx_t estimated_cardinality);

public:
	bool IsSink() const override {
		return true;
	}
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSource() const override {
		return true;
	}
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	string GetName() const override;

public:
	TableCatalogEntry &table;
	idx_t row_id_index;
	string table_path;
	vector<string> pk_names;
	vector<string> value_names;
	vector<LogicalType> value_types;
};

} // namespace duckdb
