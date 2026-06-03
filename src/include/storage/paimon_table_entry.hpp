#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "paimon_metadata.hpp"

namespace duckdb {

class PaimonTableEntry : public TableCatalogEntry {
public:
    PaimonTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                    const string &table_path, unique_ptr<PaimonTableMetadata> metadata);

    ~PaimonTableEntry() override;

    // TableCatalogEntry pure virtual overrides
    unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
    TableStorageInfo GetStorageInfo(ClientContext &context) override;

    // Table info
    string GetTablePath() const { return table_path; }
    const PaimonTableMetadata &GetMetadata() const { return *metadata; }

private:
    string table_path;
    unique_ptr<PaimonTableMetadata> metadata;
};

} // namespace duckdb
