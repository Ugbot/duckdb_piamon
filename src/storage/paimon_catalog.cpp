#include "storage/paimon_catalog.hpp"
#include "storage/paimon_schema_entry.hpp"
#include "storage/paimon_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/common/file_system.hpp"
#include "paimon_metadata.hpp"
#include "iceberg_utils.hpp"

namespace duckdb {

PaimonCatalog::PaimonCatalog(AttachedDatabase &db_p, const string &warehouse_path)
    : Catalog(db_p), warehouse_path(warehouse_path) {
}

PaimonCatalog::~PaimonCatalog() = default;

void PaimonCatalog::Initialize(bool load_builtin) {
	std::cerr << "PAIMON: PaimonCatalog::Initialize called" << std::endl;

	// Create default schema
	CreateSchemaInfo info;
	info.schema = DEFAULT_SCHEMA;
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	default_schema = make_uniq<PaimonSchemaEntry>(*this, info.schema);

	std::cerr << "PAIMON: Default schema created" << std::endl;

	// Don't load tables here - do it lazily when accessed
	// This avoids context issues during initialization

	std::cerr << "PAIMON: Initialize completed" << std::endl;
}

optional_ptr<CatalogEntry> PaimonCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
    if (info.schema == DEFAULT_SCHEMA) {
        // Default schema already exists
        return default_schema.get();
    }

    // Paimon doesn't have named schemas like traditional databases
    // All tables are in the default schema
    throw CatalogException("Paimon catalog does not support named schemas. Use default schema.");
}

void PaimonCatalog::DropSchema(ClientContext &context, DropInfo &info) {
    if (info.name == DEFAULT_SCHEMA) {
        throw CatalogException("Cannot drop the default schema");
    }
    throw CatalogException("Paimon catalog does not support named schemas");
}

optional_ptr<CatalogEntry> PaimonCatalog::CreateTable(CatalogTransaction transaction, CreateTableInfo &info) {
    if (info.schema != DEFAULT_SCHEMA) {
        throw CatalogException("Paimon tables must be created in the default schema");
    }

    // Check if table already exists
    if (TableExists(info.table)) {
        if (info.on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
            throw CatalogException("Table '%s' already exists", info.table);
        }
        return nullptr;
    }

    // Create table path
    string table_path = warehouse_path + "/" + info.table;

    // For now, create a basic table with no metadata
    // In the future, this would create the actual Paimon table structure
    auto table_metadata = make_uniq<PaimonTableMetadata>();
    table_metadata->table_format_version = "1";

    // Create default schema based on CREATE TABLE columns
    table_metadata->schema = make_uniq<PaimonSchema>();
    table_metadata->schema->id = 1;

    // Convert DuckDB columns to Paimon schema
    for (size_t i = 0; i < info.columns.size(); i++) {
        const auto &col = info.columns[i];
        PaimonSchemaField field;
        field.id = i + 1;
        field.name = col.name;

        // Convert DuckDB types to Paimon types
        switch (col.type.id()) {
            case LogicalTypeId::INTEGER:
                field.type.type_root = PaimonTypeRoot::INT;
                break;
            case LogicalTypeId::BIGINT:
                field.type.type_root = PaimonTypeRoot::LONG;
                break;
            case LogicalTypeId::VARCHAR:
                field.type.type_root = PaimonTypeRoot::STRING;
                break;
            case LogicalTypeId::BOOLEAN:
                field.type.type_root = PaimonTypeRoot::BOOLEAN;
                break;
            case LogicalTypeId::DOUBLE:
                field.type.type_root = PaimonTypeRoot::DOUBLE;
                break;
            case LogicalTypeId::FLOAT:
                field.type.type_root = PaimonTypeRoot::FLOAT;
                break;
            default:
                field.type.type_root = PaimonTypeRoot::STRING; // Fallback
                break;
        }

        table_metadata->schema->fields.push_back(std::move(field));
    }

    // Create table entry
    auto table_entry = make_uniq<PaimonTableEntry>(*this, *default_schema, info, table_path, std::move(table_metadata));

    // Add to schema
    default_schema->CreateEntry(transaction, info.table, std::move(table_entry), info.on_conflict);

    return default_schema->GetEntry(transaction, CatalogType::TABLE, info.table);
}

optional_ptr<CatalogEntry> PaimonCatalog::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
    // This would be called for CREATE TABLE AS SELECT operations
    // For now, delegate to the other CreateTable method
    throw BinderException("CREATE TABLE AS SELECT not yet supported for Paimon tables");
}

void PaimonCatalog::DropTable(ClientContext &context, DropInfo &info) {
    if (info.schema != DEFAULT_SCHEMA) {
        throw CatalogException("Paimon tables must be in the default schema");
    }

    // Remove from schema
    default_schema->DropEntry(context, info);
}

optional_ptr<CatalogEntry> PaimonCatalog::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
    throw CatalogException("Paimon catalog does not support views");
}

void PaimonCatalog::DropView(ClientContext &context, DropInfo &info) {
    throw CatalogException("Paimon catalog does not support views");
}

optional_ptr<CatalogEntry> PaimonCatalog::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
    throw CatalogException("Paimon catalog does not support functions");
}

optional_ptr<CatalogEntry> PaimonCatalog::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
    throw CatalogException("Paimon catalog does not support types");
}

optional_ptr<CatalogEntry> PaimonCatalog::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
    throw CatalogException("Paimon catalog does not support sequences");
}

optional_ptr<CatalogEntry> PaimonCatalog::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info) {
    throw CatalogException("Paimon catalog does not support indexes");
}

optional_ptr<CatalogEntry> PaimonCatalog::GetEntry(CatalogType type, const string &schema, const string &name,
                                                  OnEntryNotFound if_not_found) {
    if (schema != DEFAULT_SCHEMA) {
        if (if_not_found == OnEntryNotFound::RETURN_NULL) {
            return nullptr;
        }
        throw CatalogException("Schema '%s' does not exist", schema);
    }

    return default_schema->GetEntry(CatalogTransaction::GetSystemTransaction(context), type, name, if_not_found);
}

void PaimonCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	std::cerr << "PAIMON: ScanSchemas called" << std::endl;

	// Load tables lazily on first access
	static bool tables_loaded = false;
	if (!tables_loaded) {
		std::cerr << "PAIMON: Loading tables lazily" << std::endl;
		LoadExistingTablesWithContext(context);
		tables_loaded = true;
	}

    callback(*default_schema);
}

void PaimonCatalog::ScanEntries(void *entry_p, CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
    if (entry_p == default_schema.get()) {
        // For table scanning (SHOW TABLES), we need to scan the entries in the schema
        // The tables should have been loaded lazily during ScanSchemas
        // Use the scan without context since we don't have it here
        default_schema->Scan(type, callback);
    }
}

unique_ptr<Catalog> PaimonCatalog::Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                        AttachedDatabase &db, const string &name, AttachInfo &info,
                                        AttachOptions &options) {
    string warehouse_path = info.path;

    std::cerr << "PAIMON: PaimonCatalog::Attach called with path: " << warehouse_path << std::endl;

    // Validate warehouse path exists
    FileSystem &fs = FileSystem::GetFileSystem(context);
    if (!fs.DirectoryExists(warehouse_path)) {
        std::cerr << "PAIMON: Warehouse path does not exist: " << warehouse_path << std::endl;
        throw CatalogException("Paimon warehouse path does not exist: %s", warehouse_path);
    }

    std::cerr << "PAIMON: Creating PaimonCatalog for path: " << warehouse_path << std::endl;
    return make_uniq<PaimonCatalog>(db, warehouse_path);
}

void PaimonCatalog::LoadExistingTables() {
	// This method is called during initialization without context
	// Real loading happens in LoadExistingTablesWithContext
	std::cerr << "PAIMON: LoadExistingTables called (no context available)" << std::endl;
}

void PaimonCatalog::LoadExistingTablesWithContext(ClientContext &context) {
	std::cerr << "PAIMON: LoadExistingTablesWithContext called for warehouse: " << warehouse_path << std::endl;

	// Create a simple test table to verify catalog integration works
	std::cerr << "PAIMON: Creating test table entries for catalog verification" << std::endl;

	try {
		// Create a simple test table entry for verification
		CreateTableInfo info;
		info.schema = DEFAULT_SCHEMA;
		info.table = "test_table";

		// Add a simple column for testing
		info.columns.push_back(ColumnDefinition("id", LogicalType::BIGINT));
		info.columns.push_back(ColumnDefinition("name", LogicalType::VARCHAR));

		// Create basic metadata
		auto metadata = make_uniq<PaimonTableMetadata>();
		metadata->table_format_version = "1";
		metadata->schema = make_uniq<PaimonSchema>();

		// Add fields to schema
		PaimonSchemaField id_field;
		id_field.id = 1;
		id_field.name = "id";
		id_field.type.type_root = PaimonTypeRoot::LONG;
		metadata->schema->fields.push_back(id_field);

		PaimonSchemaField name_field;
		name_field.id = 2;
		name_field.name = "name";
		name_field.type.type_root = PaimonTypeRoot::STRING;
		metadata->schema->fields.push_back(name_field);

		// Create table entry
		string table_path = warehouse_path + "/test_table";
		auto table_entry = make_uniq<PaimonTableEntry>(*this, *default_schema, info, table_path, std::move(metadata));

		// Add to schema with proper transaction context
		default_schema->CreateEntry(CatalogTransaction::GetSystemTransaction(context), "test_table", std::move(table_entry), OnCreateConflict::ERROR_ON_CONFLICT);

		std::cerr << "PAIMON: Test table created successfully" << std::endl;

	} catch (const std::exception &e) {
		std::cerr << "PAIMON: Error creating test table: " << e.what() << std::endl;
	}

	std::cerr << "PAIMON: LoadExistingTablesWithContext completed" << std::endl;
}

PhysicalOperator &PaimonCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                    optional_ptr<PhysicalOperator> plan) {
    std::cerr << "PAIMON: PlanInsert called for table: " << op.table.name << std::endl;

    if (!plan) {
        throw NotImplementedException("INSERT INTO Paimon tables requires a data source");
    }

    // Create our PaimonInsert physical operator
    // PaimonInsert takes: LogicalOperator &op, TableCatalogEntry &table, physical_index_vector_t<idx_t> column_index_map
    auto &insert = planner.Make<PaimonInsert>(op, op.table, op.column_index_map);

    // Add the data source as a child
    insert.children.push_back(*plan);

    return insert;
}

unique_ptr<TransactionManager> PaimonCatalog::CreateTransactionManager() {
    // Create a basic transaction manager for Paimon
    // For now, we'll use DuckDB's standard transaction manager
    // TODO: Implement Paimon-specific transaction management for metadata consistency
    return make_uniq<TransactionManager>(db);
}

bool PaimonCatalog::TableExists(const string &table_name) {
    // This method is not currently used in the right context
    // For now, just return false
    return false;
}

} // namespace duckdb
