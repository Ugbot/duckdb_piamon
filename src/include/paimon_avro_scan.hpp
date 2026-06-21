#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

class ClientContext;

//! Drives DuckDB's `read_avro` table function in-process to stream an Avro file's rows as
//! DataChunks. This avoids issuing a nested SQL query (context.Query("SELECT * FROM read_avro(...)"))
//! during bind/scan, which deadlocks the engine. Used to read Paimon manifest-list / manifest /
//! index-manifest Avro files natively.
class PaimonAvroScan {
public:
	PaimonAvroScan(const string &scan_name, ClientContext &context, const string &path);

	//! Fetch the next chunk. Returns false (and leaves result empty) when the file is exhausted.
	bool GetNext(DataChunk &result);
	void InitializeChunk(DataChunk &chunk);
	bool Finished() const;

	const vector<LogicalType> &GetTypes() const {
		return return_types;
	}
	const vector<string> &GetNames() const {
		return return_names;
	}

private:
	string path;
	ClientContext &context;
	optional_ptr<TableFunction> avro_scan;
	unique_ptr<FunctionData> bind_data;
	unique_ptr<GlobalTableFunctionState> global_state;
	unique_ptr<LocalTableFunctionState> local_state;
	vector<LogicalType> return_types;
	vector<string> return_names;
	bool finished = false;
};

} // namespace duckdb
