#include "storage/paimon_catalog.hpp"
#include "storage/paimon_insert.hpp"
#include "duckdb/storage/database_size.hpp"
#include "storage/paimon_schema_entry.hpp"
#include "storage/paimon_table_entry.hpp"
#include "storage/paimon_delete.hpp"
#include "storage/paimon_update.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
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

// Build the physical insert operator for an (already-created) Paimon table. Shared by INSERT and
// CREATE TABLE AS. Append tables stream value columns through a parquet COPY; primary-key tables
// buffer the rows and write system columns themselves.
static PhysicalOperator &BuildPaimonInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                           PaimonTableEntry &paimon_table, LogicalOperator &op,
                                           PhysicalOperator &plan,
                                           physical_index_vector_t<idx_t> column_index_map) {
	string table_path = paimon_table.GetTablePath();
	auto names = paimon_table.GetColumns().GetColumnNames();
	auto types = paimon_table.GetColumns().GetColumnTypes();
	auto &metadata = paimon_table.GetMetadata();
	bool is_pk = metadata.schema && !metadata.schema->primary_keys.empty();

	if (is_pk) {
		auto &insert = planner.Make<PaimonInsert>(op, paimon_table, std::move(column_index_map));
		auto &pk_insert = insert.Cast<PaimonInsert>();
		pk_insert.pk_mode = true;
		pk_insert.pk_names = metadata.schema->primary_keys;
		pk_insert.value_names = names;
		pk_insert.value_types = types;
		insert.children.push_back(plan);
		return insert;
	}

	string data_path = table_path + "/bucket-0";
	auto &physical_copy = PaimonInsert::PlanCopyForInsert(context, planner, data_path, names, types, &plan);
	auto &insert = planner.Make<PaimonInsert>(op, paimon_table, std::move(column_index_map));
	insert.children.push_back(physical_copy);
	return insert;
}

PhysicalOperator &PaimonCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                    LogicalCreateTable &op, PhysicalOperator &plan) {
	// Create the table now (directory + schema-0), then insert the query results into it.
	auto transaction = GetCatalogTransaction(context);
	auto table_entry = op.schema.CreateTable(transaction, *op.info);
	if (!table_entry) {
		throw CatalogException("CREATE TABLE AS: table '%s' already exists", op.info->Base().table);
	}
	auto &paimon_table = table_entry->Cast<PaimonTableEntry>();
	return BuildPaimonInsert(context, planner, paimon_table, op, plan, physical_index_vector_t<idx_t>());
}

PhysicalOperator &PaimonCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                             optional_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for Paimon tables");
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not yet supported for Paimon tables");
	}
	auto &paimon_table = op.table.Cast<PaimonTableEntry>();
	return BuildPaimonInsert(context, planner, paimon_table, op, *plan, op.column_index_map);
}

PhysicalOperator &PaimonCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                             PhysicalOperator &plan) {
	auto &paimon_table = op.table.Cast<PaimonTableEntry>();
	auto &metadata = paimon_table.GetMetadata();
	if (!metadata.schema || metadata.schema->primary_keys.empty()) {
		throw NotImplementedException("UPDATE on Paimon tables requires a primary-key table");
	}
	auto names = op.table.GetColumns().GetColumnNames();
	auto types = op.table.GetColumns().GetColumnTypes();

	// Map each SET expression to the column it updates and its position in the child chunk.
	vector<string> updated_columns;
	vector<idx_t> updated_child_indexes;
	for (idx_t i = 0; i < op.expressions.size(); i++) {
		if (op.expressions[i]->GetExpressionType() != ExpressionType::BOUND_REF) {
			throw NotImplementedException("UPDATE with DEFAULT or complex SET expressions is not yet supported "
			                              "for Paimon tables");
		}
		auto &ref = op.expressions[i]->Cast<BoundReferenceExpression>();
		updated_columns.push_back(names[op.columns[i].index]);
		updated_child_indexes.push_back(ref.index);
	}

	auto &upd = planner.Make<PaimonUpdate>(op.types, op.table, 0, paimon_table.GetTablePath(),
	                                       metadata.schema->primary_keys, names, types, updated_columns,
	                                       updated_child_indexes, op.estimated_cardinality);
	upd.children.push_back(plan);
	return upd;
}

PhysicalOperator &PaimonCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                             PhysicalOperator &plan) {
	auto &paimon_table = op.table.Cast<PaimonTableEntry>();
	auto &metadata = paimon_table.GetMetadata();
	if (!metadata.schema || metadata.schema->primary_keys.empty()) {
		throw NotImplementedException("DELETE on Paimon tables requires a primary-key table");
	}
	// The bound row-id expression locates the key column produced by the child scan.
	auto &bound_ref = op.expressions[0]->Cast<BoundReferenceExpression>();
	auto names = op.table.GetColumns().GetColumnNames();
	auto types = op.table.GetColumns().GetColumnTypes();
	auto &del = planner.Make<PaimonDelete>(op.types, op.table, bound_ref.index, paimon_table.GetTablePath(),
	                                       metadata.schema->primary_keys, names, types, op.estimated_cardinality);
	del.children.push_back(plan);
	return del;
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
