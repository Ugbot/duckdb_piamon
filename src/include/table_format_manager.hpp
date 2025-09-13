//===----------------------------------------------------------------------===//
//                         DuckDB
//
// table_format_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "table_format.hpp"
#include "iceberg_table_format.hpp"
#include "paimon_table_format.hpp"
#include <memory>
#include <vector>

namespace duckdb {

class TableFormatManager {
public:
    TableFormatManager();

    // Register a table format
    void RegisterFormat(unique_ptr<TableFormat> format);

    // Get the appropriate format for a table location
    TableFormat* GetFormatForTable(const string &table_location) const;

    // Get all registered formats
    const vector<unique_ptr<TableFormat>>& GetFormats() const {
        return formats_;
    }

private:
    vector<unique_ptr<TableFormat>> formats_;
};

} // namespace duckdb
