//===----------------------------------------------------------------------===//
//                         DuckDB
//
// paimon_table_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "paimon_metadata.hpp"

namespace duckdb {

class PaimonTableEntry : public TableCatalogEntry {
public:
    PaimonTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                    const string &table_path, unique_ptr<PaimonTableMetadata> metadata);
    PaimonTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, const string &table_name,
                    const string &table_path, unique_ptr<PaimonTableMetadata> metadata);

    ~PaimonTableEntry() override;

    // TableCatalogEntry overrides
    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context) override;
    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
    TableStorageInfo GetStorageInfo(ClientContext &context) override;

    // Storage interface
    void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                              LogicalUpdate &update, ClientContext &context) override;
    unique_ptr<PhysicalOperator> CreateTableScan(LogicalGet &op, PhysicalPlanGenerator &planner) override;

    // DDL operations
    void TruncateTable(ClientContext &context) override;

    // Table info
    string GetTablePath() const { return table_path; }
    const PaimonTableMetadata &GetMetadata() const { return *metadata; }

private:
    string table_path;
    unique_ptr<PaimonTableMetadata> metadata;
};

} // namespace duckdb
