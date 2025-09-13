//===----------------------------------------------------------------------===//
//                         DuckDB
//
// iceberg_table_format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "table_format.hpp"
#include "iceberg_functions.hpp"

namespace duckdb {

class IcebergTableFormat : public TableFormat {
public:
    string GetFormatName() const override {
        return "iceberg";
    }

    bool CanHandleTable(const string &table_location) const override;

    vector<TableFunctionSet> GetTableFunctions(ExtensionLoader &loader) override {
        return IcebergFunctions::GetTableFunctions(loader);
    }
};

} // namespace duckdb
