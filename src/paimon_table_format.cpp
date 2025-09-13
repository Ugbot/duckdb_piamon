#include "paimon_table_format.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

bool PaimonTableFormat::CanHandleTable(const string &table_location) const {
	// Basic heuristic: check if path contains paimon-related directory structures
	// The actual filesystem checking is done in the table functions with proper context
	if (table_location.find("paimon") != string::npos ||
	    table_location.find("schema") != string::npos ||
	    table_location.find("snapshot") != string::npos ||
	    table_location.find("manifest") != string::npos) {
		return true;
	}

	return false;
}

} // namespace duckdb
