#include <iostream>
#include "duckdb.hpp"
int main() {
    try {
        duckdb::DuckDB db(":memory:");
        duckdb::Connection con(db);
        con.Query("LOAD parquet");
        std::cout << "Parquet loaded successfully" << std::endl;
        con.Query("LOAD '/Users/bengamble/duckdb_piamon/build/release/repository/v0.0.1/osx_arm64/paimon.duckdb_extension'");
        std::cout << "Paimon loaded successfully" << std::endl;
        auto result = con.Query("SELECT function_name FROM duckdb_functions() WHERE function_name LIKE '%paimon%'");
        std::cout << "Paimon functions: " << result->ToString() << std::endl;
        return 0;
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
