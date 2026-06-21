#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

class ClientContext;

//! Native Apache Avro Object Container File writer for Paimon manifests / manifest-lists. Encodes
//! rows (one vector<Value> per top-level record field) against the given Avro schema JSON and writes
//! an uncompressed (codec="null") OCF — so the write path no longer needs the avro extension's
//! `COPY ... (FORMAT AVRO)`. Nested records are passed as STRUCT Values, arrays as LIST Values, and
//! nullable union fields as NULL / the value.
class PaimonAvroWriter {
public:
	static void WriteFile(ClientContext &context, const string &path, const string &schema_json,
	                      const vector<vector<Value>> &rows);
};

} // namespace duckdb
