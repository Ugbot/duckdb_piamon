#include "storage/paimon_catalog.hpp"
#include "storage/paimon_insert.hpp"
#include "duckdb/storage/database_size.hpp"
#include "storage/paimon_schema_entry.hpp"
#include "storage/paimon_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "paimon_metadata.hpp"
#include "iceberg_utils.hpp"

namespace duckdb {

PaimonCatalog::PaimonCatalog(AttachedDatabase &db_p, const string &warehouse_path)
    : Catalog(db_p), warehouse_path(warehouse_path) {
}

PaimonCatalog::~PaimonCatalog() = default;

void PaimonCatalog::Initialize(bool load_builtin) {
	CreateSchemaInfo info;
	info.schema = DEFAULT_SCHEMA;
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	default_schema = make_uniq<PaimonSchemaEntry>(*this, info);
}

optional_ptr<CatalogEntry> PaimonCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	if (info.schema == DEFAULT_SCHEMA) {
		return default_schema.get();
	}
	throw CatalogException("Paimon catalog does not support named schemas. Use default schema.");
}

void PaimonCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	if (info.name == DEFAULT_SCHEMA) {
		throw CatalogException("Cannot drop the default schema");
	}
	throw CatalogException("Paimon catalog does not support named schemas");
}

optional_ptr<SchemaCatalogEntry> PaimonCatalog::LookupSchema(CatalogTransaction transaction,
                                                              const EntryLookupInfo &schema_lookup,
                                                              OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();
	if (schema_name == DEFAULT_SCHEMA || schema_name.empty()) {
		return default_schema.get();
	}
	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		return nullptr;
	}
	throw CatalogException("Schema '%s' does not exist in Paimon catalog", schema_name);
}

void PaimonCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	callback(*default_schema);
}

PhysicalOperator &PaimonCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                    LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("CREATE TABLE AS not yet supported for Paimon tables");
}

PhysicalOperator &PaimonCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                             optional_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for Paimon tables");
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not yet supported for Paimon tables");
	}

	auto &table_entry = op.table;
	// Use the absolute table directory (warehouse/<table>), not the bare table name.
	auto &paimon_table = table_entry.Cast<PaimonTableEntry>();
	string table_path = paimon_table.GetTablePath();

	// Get the column names and types
	auto names = table_entry.GetColumns().GetColumnNames();
	auto types = table_entry.GetColumns().GetColumnTypes();

	auto &metadata = paimon_table.GetMetadata();
	bool is_pk = metadata.schema && !metadata.schema->primary_keys.empty();

	if (is_pk) {
		// Primary-key tables: the insert operator buffers the raw rows and writes the data file with
		// Paimon system columns (_KEY_*, _SEQUENCE_NUMBER, _VALUE_KIND) itself, then merge-on-read
		// resolves the latest row per key. Feed the data plan directly (no upstream COPY).
		auto &insert = planner.Make<PaimonInsert>(op, table_entry, op.column_index_map);
		auto &pk_insert = insert.Cast<PaimonInsert>();
		pk_insert.pk_mode = true;
		pk_insert.pk_names = metadata.schema->primary_keys;
		pk_insert.value_names = names;
		pk_insert.value_types = types;
		insert.children.push_back(*plan);
		return insert;
	}

	// Append tables: stream value columns through a parquet COPY, then commit.
	string data_path = table_path + "/bucket-0";
	auto &physical_copy = PaimonInsert::PlanCopyForInsert(context, planner, data_path, names, types, plan);
	auto &insert = planner.Make<PaimonInsert>(op, table_entry, op.column_index_map);
	insert.children.push_back(physical_copy);
	return insert;
}

PhysicalOperator &PaimonCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                             PhysicalOperator &plan) {
	throw NotImplementedException("UPDATE not yet supported for Paimon tables");
}

PhysicalOperator &PaimonCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                             PhysicalOperator &plan) {
	throw NotImplementedException("DELETE not yet supported for Paimon tables");
}

DatabaseSize PaimonCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize result;
	result.free_blocks = 0;
	result.total_blocks = 0;
	result.used_blocks = 0;
	result.wal_size = 0;
	result.block_size = 0;
	result.bytes = 0;
	return result;
}

bool PaimonCatalog::InMemory() {
	return false;
}

string PaimonCatalog::GetDBPath() {
	return warehouse_path;
}

unique_ptr<Catalog> PaimonCatalog::Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                          AttachedDatabase &db, const string &name, AttachInfo &info,
                                          AttachOptions &options) {
	string warehouse_path = info.path;

	FileSystem &fs = FileSystem::GetFileSystem(context);
	if (!fs.DirectoryExists(warehouse_path)) {
		throw CatalogException("Paimon warehouse path does not exist: %s", warehouse_path);
	}

	return make_uniq<PaimonCatalog>(db, warehouse_path);
}

void PaimonCatalog::LoadExistingTablesWithContext(ClientContext &context) {
	// Lazy table loading - scan warehouse for Paimon tables
	// Tables are directories with a snapshot/ subdirectory
}

} // namespace duckdb
