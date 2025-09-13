#include "storage/paimon_schema_entry.hpp"
#include "storage/paimon_table_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"

namespace duckdb {

PaimonSchemaEntry::PaimonSchemaEntry(Catalog &catalog, CreateSchemaInfo &info)
    : SchemaCatalogEntry(catalog, info.schema, true) {
}

PaimonSchemaEntry::PaimonSchemaEntry(Catalog &catalog, const string &name)
    : SchemaCatalogEntry(catalog, name, true) {
}

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
    // This would be called for CREATE TABLE AS SELECT
    // Delegate to catalog
    return catalog.CreateTable(transaction, info);
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

optional_ptr<CatalogEntry> PaimonSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info) {
    throw CatalogException("Paimon catalog does not support indexes");
}

void PaimonSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
    // For tables, we need to handle the actual file system cleanup
    if (info.type == CatalogType::TABLE) {
        // TODO: Clean up Paimon table files when dropping
        // For now, just remove from catalog
    }

    SchemaCatalogEntry::DropEntry(context, info);
}

void PaimonSchemaEntry::AlterEntry(ClientContext &context, AlterInfo &info) {
    throw CatalogException("Paimon catalog does not support ALTER operations");
}

void PaimonSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
    // Scan through our entries
    entries.Scan([&](CatalogEntry &entry) {
        if (entry.type == type) {
            callback(entry);
        }
    });
}

void PaimonSchemaEntry::Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
    // Scan through our entries
    entries.Scan([&](CatalogEntry &entry) {
        if (entry.type == type) {
            callback(entry);
        }
    });
}

} // namespace duckdb
