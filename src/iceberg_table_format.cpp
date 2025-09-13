#include "iceberg_table_format.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

bool IcebergTableFormat::CanHandleTable(const string &table_location) const {
	// Basic heuristic: check if path contains iceberg-related directory structures
	// The actual filesystem checking is done in the table functions with proper context
	if (table_location.find("iceberg") != string::npos ||
	    table_location.find("metadata") != string::npos) {
		return true;
	}

	return false;
}

} // namespace duckdb
