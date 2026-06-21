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
    //! Resolve the paimon_scan table function bound on this table's path (catalog-integrated scan)
    TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                  const EntryLookupInfo &lookup_info) override;
    TableStorageInfo GetStorageInfo(ClientContext &context) override;
    //! The row-id virtual column carries the primary key (so DELETE/UPDATE identify rows by key):
    //! the key column's type for a single-column PK, or a STRUCT of the key columns for a composite
    //! PK. Falls back to BIGINT (a row sequence) when there is no primary key.
    virtual_column_map_t GetVirtualColumns() const override;

    // Table info
    string GetTablePath() const { return table_path; }
    const PaimonTableMetadata &GetMetadata() const { return *metadata; }

private:
    string table_path;
    unique_ptr<PaimonTableMetadata> metadata;
};

} // namespace duckdb
