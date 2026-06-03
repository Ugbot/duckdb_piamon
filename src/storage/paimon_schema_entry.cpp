#include "storage/paimon_schema_entry.hpp"
#include "storage/paimon_table_entry.hpp"
#include "storage/paimon_catalog.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "paimon_metadata.hpp"
#include <fstream>
#include <ctime>

namespace duckdb {

// Forward declarations
static PaimonDataType DuckDBTypeToPaimonDataType(const LogicalType &type);
static string CreateSchemaJson(const PaimonSchema &schema, int schema_id);

PaimonSchemaEntry::PaimonSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info) {
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &paimon_catalog = catalog.Cast<PaimonCatalog>();
	string warehouse_path = paimon_catalog.GetDBPath();
	string table_path = warehouse_path + "/" + info.Base().table;

	// Get the client context to access the filesystem
	ClientContext &context = *transaction.context;

	FileSystem &fs = FileSystem::GetFileSystem(context);

	// Create the table directory
	if (!fs.DirectoryExists(table_path)) {
		fs.CreateDirectory(table_path);
	}

	// Create required subdirectories
	string manifest_dir = table_path + "/manifest";
	string snapshot_dir = table_path + "/snapshot";
	string data_dir = table_path + "/bucket-0";

	if (!fs.DirectoryExists(manifest_dir)) {
		fs.CreateDirectory(manifest_dir);
	}
	if (!fs.DirectoryExists(snapshot_dir)) {
		fs.CreateDirectory(snapshot_dir);
	}
	if (!fs.DirectoryExists(data_dir)) {
		fs.CreateDirectory(data_dir);
	}

	// Create initial schema.json
	PaimonSchema schema;
	schema.id = 0;
	schema.partition_keys.clear();
	schema.primary_keys.clear();

	// Convert DuckDB columns to Paimon schema fields
	auto &columns = info.Base().columns;
	for (idx_t i = 0; i < columns.LogicalColumnCount(); i++) {
		auto &col = columns.GetColumn(LogicalIndex(i));
		PaimonSchemaField field;
		field.id = i;
		field.name = col.Name();
		// Default to nullable=true unless explicitly constrained (DuckDB defaults to nullable)
		field.nullable = true;

		// Convert DuckDB LogicalType to PaimonDataType
		auto type = col.Type();
		field.type = DuckDBTypeToPaimonDataType(type);

		schema.fields.push_back(field);
	}

	// Write metadata.json with initial schema
	string metadata_json = "{\n";
	metadata_json += "  \"metaVersion\": \"2\",\n";
	metadata_json += "  \"version\": 2,\n";
	metadata_json += "  \"id\": 2,\n";
	metadata_json += "  \"schemaId\": 0,\n";
	metadata_json += "  \"snapshots\": [],\n";
	metadata_json += "  \"currentSnapshotId\": null,\n";
	metadata_json += "  \"lastSequenceNumber\": 0\n";
	metadata_json += "}\n";

	string metadata_path = table_path + "/metadata.json";
	auto handle = fs.OpenFile(metadata_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	handle->Write((void *)metadata_json.c_str(), metadata_json.size());

	// Create the initial snapshot for schema version 0
	string schema_0_json = CreateSchemaJson(schema, 0);
	string schema_path = table_path + "/schema/" + std::to_string(schema.id) + ".json";
	string schema_dir = table_path + "/schema";
	if (!fs.DirectoryExists(schema_dir)) {
		fs.CreateDirectory(schema_dir);
	}

	auto schema_handle = fs.OpenFile(schema_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	schema_handle->Write((void *)schema_0_json.c_str(), schema_0_json.size());

	// Create empty table metadata
	auto table_metadata = make_uniq<PaimonTableMetadata>();
	table_metadata->table_location = table_path;
	table_metadata->schema = make_uniq<PaimonSchema>(schema);

	// Create the table entry
	auto result = make_uniq<PaimonTableEntry>(catalog, *this, info.Base(), table_path, std::move(table_metadata));
	return result.release();
}

// Helper function to convert DuckDB LogicalType to PaimonDataType
static PaimonDataType DuckDBTypeToPaimonDataType(const LogicalType &type) {
	PaimonDataType result;

	switch (type.id()) {
		case LogicalTypeId::BOOLEAN:
			result.type_root = PaimonTypeRoot::BOOLEAN;
			break;
		case LogicalTypeId::TINYINT:
		case LogicalTypeId::SMALLINT:
		case LogicalTypeId::INTEGER:
			result.type_root = PaimonTypeRoot::INT;
			break;
		case LogicalTypeId::BIGINT:
			result.type_root = PaimonTypeRoot::LONG;
			break;
		case LogicalTypeId::FLOAT:
			result.type_root = PaimonTypeRoot::FLOAT;
			break;
		case LogicalTypeId::DOUBLE:
			result.type_root = PaimonTypeRoot::DOUBLE;
			break;
		case LogicalTypeId::VARCHAR:
			result.type_root = PaimonTypeRoot::STRING;
			break;
		case LogicalTypeId::DATE:
			result.type_root = PaimonTypeRoot::DATE;
			break;
		case LogicalTypeId::TIMESTAMP:
		case LogicalTypeId::TIMESTAMP_TZ:
			result.type_root = PaimonTypeRoot::TIMESTAMP;
			break;
		case LogicalTypeId::BLOB:
			result.type_root = PaimonTypeRoot::BINARY;
			break;
		case LogicalTypeId::DECIMAL:
			result.type_root = PaimonTypeRoot::DECIMAL;
			result.precision = DecimalType::GetWidth(type);
			result.scale = DecimalType::GetScale(type);
			break;
		default:
			// Default to string for unsupported types
			result.type_root = PaimonTypeRoot::STRING;
	}

	return result;
}

// Helper to create schema JSON
static string CreateSchemaJson(const PaimonSchema &schema, int schema_id) {
	string json = "{\n";
	json += "  \"id\": " + std::to_string(schema.id) + ",\n";
	json += "  \"fields\": [\n";

	for (idx_t i = 0; i < schema.fields.size(); i++) {
		const auto &field = schema.fields[i];
		if (i > 0) json += ",\n";
		json += "    {\n";
		json += "      \"id\": " + std::to_string(field.id) + ",\n";
		json += "      \"name\": \"" + field.name + "\",\n";
		json += "      \"type\": \"" + field.type.ToString() + "\",\n";
		json += "      \"required\": " + string(field.nullable ? "false" : "true") + "\n";
		json += "    }";
	}

	json += "\n  ],\n";
	json += "  \"partitionKeys\": [],\n";
	json += "  \"primaryKeys\": [],\n";
	json += "  \"comment\": \"\"\n";
	json += "}\n";

	return json;
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw CatalogException("Paimon catalog does not support views");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw CatalogException("Paimon catalog does not support functions");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw CatalogException("Paimon catalog does not support types");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw CatalogException("Paimon catalog does not support sequences");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                           TableCatalogEntry &table) {
	throw CatalogException("Paimon catalog does not support indexes");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	throw CatalogException("Paimon catalog does not support collations");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                   CreateTableFunctionInfo &info) {
	throw CatalogException("Paimon catalog does not support table functions");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                  CreateCopyFunctionInfo &info) {
	throw CatalogException("Paimon catalog does not support copy functions");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                    CreatePragmaFunctionInfo &info) {
	throw CatalogException("Paimon catalog does not support pragma functions");
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                           const EntryLookupInfo &lookup_info) {
	return nullptr;
}

void PaimonSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	throw CatalogException("DROP not yet supported for Paimon catalog");
}

void PaimonSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw CatalogException("ALTER not supported for Paimon catalog");
}

void PaimonSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
}

void PaimonSchemaEntry::Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	Scan(type, callback);
}

} // namespace duckdb
