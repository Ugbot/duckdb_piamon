#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class PaimonSchemaEntry;

class PaimonCatalog : public Catalog {
public:
    PaimonCatalog(AttachedDatabase &db_p, const string &warehouse_path);
    ~PaimonCatalog() override;

    // Pure virtual overrides from Catalog
    void Initialize(bool load_builtin) override;
    string GetCatalogType() override { return "paimon"; }

    optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
    void DropSchema(ClientContext &context, DropInfo &info) override;

    optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction,
                                                   const EntryLookupInfo &schema_lookup,
                                                   OnEntryNotFound if_not_found) override;

    void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

    PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                        LogicalCreateTable &op, PhysicalOperator &plan) override;
    PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                 optional_ptr<PhysicalOperator> plan) override;
    PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                 PhysicalOperator &plan) override;
    PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                 PhysicalOperator &plan) override;

    DatabaseSize GetDatabaseSize(ClientContext &context) override;
    bool InMemory() override;
    string GetDBPath() override;

    // Static attach method for StorageExtension
    static unique_ptr<Catalog> Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                    AttachedDatabase &db, const string &name, AttachInfo &info,
                                    AttachOptions &options);

private:
    string warehouse_path;
    unique_ptr<PaimonSchemaEntry> default_schema;

    void LoadExistingTablesWithContext(ClientContext &context);
};

} // namespace duckdb
