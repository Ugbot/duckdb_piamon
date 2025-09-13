//===----------------------------------------------------------------------===//
//                         DuckDB
//
// paimon_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {
class ExtensionLoader;

class PaimonFunctions {
public:
    static vector<TableFunctionSet> GetTableFunctions(ExtensionLoader &loader);
    static vector<ScalarFunction> GetScalarFunctions();

private:
    static TableFunctionSet GetPaimonSnapshotsFunction();
    static TableFunctionSet GetPaimonScanFunction(ExtensionLoader &instance);
    static TableFunctionSet GetPaimonMetadataFunction();
    static TableFunctionSet GetPaimonCreateTableFunction();
    static TableFunctionSet GetPaimonInsertFunction();
    static TableFunctionSet GetPaimonAttachFunction();

    // Simple test function
    static void PaimonTestFunction(DataChunk &args, ExpressionState &state, Vector &result);
};

} // namespace duckdb
