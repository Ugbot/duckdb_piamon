#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"

namespace duckdb {

class PhysicalPlanGenerator;

struct PaimonWrittenFile {
	string file_path;      // Relative path within table directory
	int64_t row_count = 0;
	int64_t file_size = 0;
	int32_t bucket = 0;
};

class PaimonInsert : public PhysicalOperator {
public:
	PaimonInsert(PhysicalPlan &physical_plan, LogicalOperator &op, TableCatalogEntry &table,
	             physical_index_vector_t<idx_t> column_index_map_p);

public:
	// Sink interface
	bool IsSink() const override;
	bool ParallelSink() const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	// Source interface (returns the inserted row count)
	bool IsSource() const override {
		return true;
	}
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	PhysicalOperatorType GetOperatorType() const {
		return PhysicalOperatorType::EXTENSION;
	}

	//! Plan the COPY TO FILE operator that writes parquet files
	static PhysicalOperator &PlanCopyForInsert(ClientContext &context, PhysicalPlanGenerator &planner,
	                                           const string &data_path, const vector<string> &names,
	                                           const vector<LogicalType> &types, optional_ptr<PhysicalOperator> plan);

public:
	//! Create manifest + manifest-list + snapshot after writing.
	//! Shared by INSERT and DELETE/UPDATE (which add data files / tombstones) and by compaction.
	//! carry_forward=true accumulates prior manifests into the base list (normal append/upsert);
	//! carry_forward=false replaces the active set with just `written_files` (compaction/overwrite).
	static void CommitWrittenFiles(ClientContext &context, const string &table_path,
	                               const vector<PaimonWrittenFile> &written_files, idx_t total_rows,
	                               bool carry_forward = true, const string &commit_kind = "APPEND");

private:

	//! Write a primary-key data file (value columns + _KEY_*, _SEQUENCE_NUMBER, _VALUE_KIND system
	//! columns) from buffered rows, then commit. Returns the written-file metadata.
	void CommitPrimaryKeyRows(ClientContext &context, const string &table_path, ColumnDataCollection &rows) const;

public:
	optional_ptr<TableCatalogEntry> table;
	physical_index_vector_t<idx_t> column_index_map;
	//! Primary-key write mode: the operator buffers raw rows and writes system columns itself
	//! (instead of receiving file metadata from an upstream COPY).
	bool pk_mode = false;
	vector<string> pk_names;
	vector<string> value_names;
	vector<LogicalType> value_types;
};

} // namespace duckdb
