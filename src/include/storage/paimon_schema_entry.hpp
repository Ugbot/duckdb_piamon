//===----------------------------------------------------------------------===//
//                         DuckDB
//
// paimon_schema_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

class PaimonSchemaEntry : public SchemaCatalogEntry {
public:
    PaimonSchemaEntry(Catalog &catalog, CreateSchemaInfo &info);
    PaimonSchemaEntry(Catalog &catalog, const string &name);

    // SchemaCatalogEntry overrides
    optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
    optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
    optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
    optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
    optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
    optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info) override;

    void DropEntry(ClientContext &context, DropInfo &info) override;
    void AlterEntry(ClientContext &context, AlterInfo &info) override;

    // Scanning methods
    void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
};

} // namespace duckdb
