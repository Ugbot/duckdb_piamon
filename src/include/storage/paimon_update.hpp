#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

//! Physical UPDATE for primary-key Paimon tables. The child plan produces, per matched row, the new
//! values of the updated columns plus the row-id (= primary-key value). UPDATE is realized as an
//! upsert: at Finalize we join the table's current rows (via paimon_scan) with the buffered updates,
//! overwrite the updated columns, and write the full rows back as INSERT records at a higher
//! sequence number (merge-on-read keeps the latest).
class PaimonUpdate : public PhysicalOperator {
public:
	PaimonUpdate(PhysicalPlan &physical_plan, vector<LogicalType> types, TableCatalogEntry &table, idx_t row_id_index,
	             string table_path, vector<string> pk_names, vector<string> value_names,
	             vector<LogicalType> value_types, vector<string> updated_columns, vector<idx_t> updated_child_indexes,
	             idx_t estimated_cardinality);

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
	vector<string> updated_columns;       //! names of the columns being SET
	vector<idx_t> updated_child_indexes;  //! child-chunk index of each SET value
};

} // namespace duckdb
