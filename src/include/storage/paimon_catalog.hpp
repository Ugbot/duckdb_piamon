//===----------------------------------------------------------------------===//
//                         DuckDB
//
// paimon_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

class PaimonCatalog : public Catalog {
public:
    PaimonCatalog(AttachedDatabase &db_p, const string &warehouse_path);
    ~PaimonCatalog() override;

    // Catalog API
    void Initialize(bool load_builtin) override;
    string GetCatalogType() override { return "paimon"; }

    // Schema operations
    optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
    void DropSchema(ClientContext &context, DropInfo &info) override;

    // Table operations
    optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, CreateTableInfo &info) override;
    optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
    void DropTable(ClientContext &context, DropInfo &info) override;

    // View operations (not supported for Paimon)
    optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
    void DropView(ClientContext &context, DropInfo &info) override;

    // Function operations (not supported)
    optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;

    // Type operations (not supported)
    optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;

    // Sequence operations (not supported)
    optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;

    // Index operations (not supported)
    optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info) override;

    // Lookup operations
    optional_ptr<CatalogEntry> GetEntry(CatalogType type, const string &schema, const string &name,
                                      OnEntryNotFound if_not_found = OnEntryNotFound::RETURN_NULL) override;

    // Schema scanning
    void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

    // Entry scanning
    void ScanEntries(void *entry_p, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;

    // Physical planning
    PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                 optional_ptr<PhysicalOperator> plan) override;
    PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                 optional_ptr<PhysicalOperator> plan) override;
    PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                 optional_ptr<PhysicalOperator> plan) override;

    // Transaction management
    unique_ptr<TransactionManager> CreateTransactionManager() override;

    // Static attach method
    static unique_ptr<Catalog> Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                    AttachedDatabase &db, const string &name, AttachInfo &info,
                                    AttachOptions &options);

private:
    string warehouse_path;
    unique_ptr<SchemaCatalogEntry> default_schema;

    // Helper methods
    void LoadExistingTables();
    bool TableExists(const string &table_name);
};

} // namespace duckdb
