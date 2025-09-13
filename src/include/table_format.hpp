//===----------------------------------------------------------------------===//
//                         DuckDB
//
// table_format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/string.hpp"

#include <memory>
#include <vector>

namespace duckdb {

class TableFormat {
public:
    virtual ~TableFormat() = default;

    // Get the name of this table format (e.g., "iceberg", "paimon")
    virtual string GetFormatName() const = 0;

    // Check if this format can handle the given table location/metadata
    virtual bool CanHandleTable(const string &table_location) const = 0;

    // Get the format-specific table functions
    virtual vector<TableFunctionSet> GetTableFunctions(class ExtensionLoader &loader) = 0;
};

} // namespace duckdb
