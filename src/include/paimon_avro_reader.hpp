#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {

class ClientContext;

//! Native Apache Avro Object Container File reader for Paimon manifests / manifest-lists /
//! index-manifests. Reads the file directly (no `read_avro` SQL function), supporting the `null`
//! (uncompressed) and `zstandard` block codecs Paimon uses — notably zstandard, which the DuckDB
//! avro extension cannot decode. Each top-level record becomes a row of Values (nested Avro records
//! become STRUCT Values, arrays become LIST Values), matching what the manifest parsers consume.
class PaimonAvroReader {
public:
	//! Read the whole Avro file. Throws on malformed input or an unsupported codec.
	PaimonAvroReader(ClientContext &context, const string &path);

	const vector<string> &GetNames() const {
		return column_names;
	}
	idx_t RowCount() const {
		return rows.size();
	}
	//! Value of top-level column `col` in row `row`.
	const Value &GetValue(idx_t row, idx_t col) const {
		return rows[row][col];
	}

private:
	vector<string> column_names;
	vector<vector<Value>> rows;
};

} // namespace duckdb
