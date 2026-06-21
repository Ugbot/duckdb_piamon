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
#include "duckdb/parser/column_definition.hpp"
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

	// Write the initial Paimon schema file: {table}/schema/schema-0 (no extension, Paimon JSON).
	string schema_dir = table_path + "/schema";
	if (!fs.DirectoryExists(schema_dir)) {
		fs.CreateDirectory(schema_dir);
	}
	string schema_0_json = CreateSchemaJson(schema, 0);
	string schema_path = schema_dir + "/schema-" + std::to_string(schema.id);
	auto schema_handle = fs.OpenFile(schema_path, FileFlags::FILE_FLAGS_WRITE | FileFlags::FILE_FLAGS_FILE_CREATE);
	schema_handle->Write((void *)schema_0_json.c_str(), schema_0_json.size());

	// Create empty table metadata
	auto table_metadata = make_uniq<PaimonTableMetadata>();
	table_metadata->table_location = table_path;
	table_metadata->schema = make_uniq<PaimonSchema>(schema);

	// Create the table entry and retain ownership in this schema (filesystem catalog has no catalog set).
	auto result = make_uniq<PaimonTableEntry>(catalog, *this, info.Base(), table_path, std::move(table_metadata));
	auto entry_ptr = result.get();
	{
		lock_guard<mutex> guard(entry_lock);
		tables[info.Base().table] = std::move(result);
	}
	return entry_ptr;
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

// Helper to create a Paimon schema file (JSON). Mirrors the format pypaimon/Flink write:
// version, id, fields[{id,name,type}], highestFieldId, partitionKeys, primaryKeys, options.
static string CreateSchemaJson(const PaimonSchema &schema, int schema_id) {
	auto json_array = [](const vector<string> &items) {
		string out = "[";
		for (idx_t i = 0; i < items.size(); i++) {
			if (i > 0) {
				out += ", ";
			}
			out += "\"" + items[i] + "\"";
		}
		return out + "]";
	};

	int highest_field_id = 0;
	string json = "{\n";
	json += "  \"version\": 3,\n";
	json += "  \"id\": " + std::to_string(schema_id) + ",\n";
	json += "  \"fields\": [\n";
	for (idx_t i = 0; i < schema.fields.size(); i++) {
		const auto &field = schema.fields[i];
		if (field.id > highest_field_id) {
			highest_field_id = field.id;
		}
		string type_str = field.type.ToString();
		if (!field.nullable) {
			type_str += " NOT NULL";
		}
		if (i > 0) {
			json += ",\n";
		}
		json += "    {\n";
		json += "      \"id\": " + std::to_string(field.id) + ",\n";
		json += "      \"name\": \"" + field.name + "\",\n";
		json += "      \"type\": \"" + type_str + "\"\n";
		json += "    }";
	}
	json += "\n  ],\n";
	json += "  \"highestFieldId\": " + std::to_string(highest_field_id) + ",\n";
	json += "  \"partitionKeys\": " + json_array(schema.partition_keys) + ",\n";
	json += "  \"primaryKeys\": " + json_array(schema.primary_keys) + ",\n";
	json += "  \"options\": {},\n";
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

optional_ptr<CatalogEntry> PaimonSchemaEntry::LoadTable(ClientContext &context, const string &table_name) {
	auto &paimon_catalog = catalog.Cast<PaimonCatalog>();
	string table_path = paimon_catalog.GetDBPath() + "/" + table_name;

	FileSystem &fs = FileSystem::GetFileSystem(context);
	// A Paimon table must have a schema/ directory; the snapshot/ dir only appears after the first commit.
	if (!fs.DirectoryExists(table_path) || !fs.DirectoryExists(table_path + "/schema")) {
		return nullptr;
	}

	// Load metadata from the latest snapshot (or schema-only for freshly created, never-committed tables).
	PaimonOptions options;
	unique_ptr<PaimonTableMetadata> metadata;
	try {
		auto meta_path = PaimonTableMetadata::GetMetaDataPath(context, table_path, fs, options);
		metadata = PaimonTableMetadata::Parse(meta_path, fs, options.metadata_compression_codec);
	} catch (const std::exception &e) {
		// No committed snapshot yet (e.g. freshly created, never-written table) — not queryable yet.
		return nullptr;
	}
	if (!metadata || !metadata->schema) {
		return nullptr;
	}

	// Build the DuckDB column list from the Paimon schema.
	CreateTableInfo info;
	info.schema = name;
	info.table = table_name;
	for (auto &field : metadata->schema->fields) {
		info.columns.AddColumn(ColumnDefinition(field.name, PaimonTypeToLogicalType(field.type)));
	}

	auto entry = make_uniq<PaimonTableEntry>(catalog, *this, info, table_path, std::move(metadata));
	auto entry_ptr = entry.get();
	tables[table_name] = std::move(entry);
	return entry_ptr;
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                           const EntryLookupInfo &lookup_info) {
	// Only tables are materialized in a Paimon catalog.
	if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}
	auto &table_name = lookup_info.GetEntryName();

	lock_guard<mutex> guard(entry_lock);
	auto it = tables.find(table_name);
	if (it != tables.end()) {
		return it->second.get();
	}
	if (!transaction.context) {
		return nullptr;
	}
	return LoadTable(*transaction.context, table_name);
}

void PaimonSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	throw CatalogException("DROP not yet supported for Paimon catalog");
}

void PaimonSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw CatalogException("ALTER not supported for Paimon catalog");
}

void PaimonSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	// Without a client context we can only surface entries already cached.
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	lock_guard<mutex> guard(entry_lock);
	for (auto &kv : tables) {
		callback(*kv.second);
	}
}

void PaimonSchemaEntry::Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	auto &paimon_catalog = catalog.Cast<PaimonCatalog>();
	FileSystem &fs = FileSystem::GetFileSystem(context);
	string warehouse = paimon_catalog.GetDBPath();

	// Discover Paimon tables on disk (directories containing a schema/ subdir) and ensure each is loaded.
	if (fs.DirectoryExists(warehouse)) {
		vector<string> table_names;
		fs.ListFiles(warehouse, [&](const string &fname, bool is_dir) {
			if (is_dir && fs.DirectoryExists(warehouse + "/" + fname + "/schema")) {
				table_names.push_back(fname);
			}
		});
		lock_guard<mutex> guard(entry_lock);
		for (auto &table_name : table_names) {
			if (tables.find(table_name) == tables.end()) {
				LoadTable(context, table_name);
			}
		}
	}

	lock_guard<mutex> guard(entry_lock);
	for (auto &kv : tables) {
		callback(*kv.second);
	}
}

} // namespace duckdb
