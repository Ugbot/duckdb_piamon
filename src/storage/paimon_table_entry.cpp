#include "storage/paimon_table_entry.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "paimon_functions.hpp"

namespace duckdb {

PaimonTableEntry::PaimonTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                  const string &table_path, unique_ptr<PaimonTableMetadata> metadata)
    : TableCatalogEntry(catalog, schema, info), table_path(table_path), metadata(std::move(metadata)) {

    // Set up columns based on Paimon schema
    if (this->metadata->schema) {
        for (const auto &field : this->metadata->schema->fields) {
            ColumnDefinition col_def(field.name, LogicalType::VARCHAR); // Default to VARCHAR for now

            // Convert Paimon types to DuckDB types
            switch (field.type.type_root) {
                case PaimonTypeRoot::STRING:
                case PaimonTypeRoot::VARCHAR:
                    col_def.type = LogicalType::VARCHAR;
                    break;
                case PaimonTypeRoot::INT:
                    col_def.type = LogicalType::INTEGER;
                    break;
                case PaimonTypeRoot::LONG:
                case PaimonTypeRoot::BIGINT:
                    col_def.type = LogicalType::BIGINT;
                    break;
                case PaimonTypeRoot::BOOLEAN:
                    col_def.type = LogicalType::BOOLEAN;
                    break;
                case PaimonTypeRoot::FLOAT:
                    col_def.type = LogicalType::FLOAT;
                    break;
                case PaimonTypeRoot::DOUBLE:
                    col_def.type = LogicalType::DOUBLE;
                    break;
                default:
                    col_def.type = LogicalType::VARCHAR; // Fallback
                    break;
            }

            columns.push_back(std::move(col_def));
        }
    }
}

PaimonTableEntry::PaimonTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, const string &table_name,
                                  const string &table_path, unique_ptr<PaimonTableMetadata> metadata)
    : TableCatalogEntry(catalog, schema, table_name), table_path(table_path), metadata(std::move(metadata)) {

    // Set up columns based on Paimon schema
    if (this->metadata->schema) {
        for (const auto &field : this->metadata->schema->fields) {
            ColumnDefinition col_def(field.name, LogicalType::VARCHAR); // Default to VARCHAR

            // Convert Paimon types to DuckDB types
            switch (field.type.type_root) {
                case PaimonTypeRoot::STRING:
                case PaimonTypeRoot::VARCHAR:
                    col_def.type = LogicalType::VARCHAR;
                    break;
                case PaimonTypeRoot::INT:
                    col_def.type = LogicalType::INTEGER;
                    break;
                case PaimonTypeRoot::LONG:
                case PaimonTypeRoot::BIGINT:
                    col_def.type = LogicalType::BIGINT;
                    break;
                case PaimonTypeRoot::BOOLEAN:
                    col_def.type = LogicalType::BOOLEAN;
                    break;
                case PaimonTypeRoot::FLOAT:
                    col_def.type = LogicalType::FLOAT;
                    break;
                case PaimonTypeRoot::DOUBLE:
                    col_def.type = LogicalType::DOUBLE;
                    break;
                default:
                    col_def.type = LogicalType::VARCHAR; // Fallback
                    break;
            }

            columns.push_back(std::move(col_def));
        }
    }
}

PaimonTableEntry::~PaimonTableEntry() = default;

unique_ptr<BaseStatistics> PaimonTableEntry::GetStatistics(ClientContext &context) {
    // TODO: Implement statistics collection
    return nullptr;
}

TableFunction PaimonTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
    // Return the paimon_scan table function
    // This would need to be implemented to work with the catalog
    throw InternalException("Paimon table scan not yet implemented in catalog");
}

TableStorageInfo PaimonTableEntry::GetStorageInfo(ClientContext &context) {
    TableStorageInfo result;
    result.cardinality = 0; // Unknown
    result.index_info = vector<IndexInfo>();
    return result;
}

void PaimonTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                           LogicalUpdate &update, ClientContext &context) {
    throw BinderException("Paimon tables do not support UPDATE operations yet");
}

unique_ptr<PhysicalOperator> PaimonTableEntry::CreateTableScan(LogicalGet &op, PhysicalPlanGenerator &planner) {
    // This would create a physical operator for scanning the Paimon table
    // For now, throw an exception
    throw InternalException("Paimon table scan not yet implemented");
}

void PaimonTableEntry::TruncateTable(ClientContext &context) {
    throw CatalogException("Paimon tables do not support TRUNCATE operations yet");
}

} // namespace duckdb
