//===----------------------------------------------------------------------===//
//                         DuckDB
//
// paimon_table_format.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "table_format.hpp"
#include "paimon_functions.hpp"

namespace duckdb {

class PaimonTableFormat : public TableFormat {
public:
    string GetFormatName() const override {
        return "paimon";
    }

    bool CanHandleTable(const string &table_location) const override;

    vector<TableFunctionSet> GetTableFunctions(ExtensionLoader &loader) override {
        return PaimonFunctions::GetTableFunctions(loader);
    }
};

} // namespace duckdb
