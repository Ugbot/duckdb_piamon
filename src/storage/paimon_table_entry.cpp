#include "storage/paimon_table_entry.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

PaimonTableEntry::PaimonTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                   const string &table_path, unique_ptr<PaimonTableMetadata> metadata)
    : TableCatalogEntry(catalog, schema, info), table_path(table_path), metadata(std::move(metadata)) {
}

PaimonTableEntry::~PaimonTableEntry() = default;

unique_ptr<BaseStatistics> PaimonTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

TableFunction PaimonTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	throw InternalException("Paimon table scan through catalog not yet implemented");
}

TableStorageInfo PaimonTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	result.cardinality = 0;
	return result;
}

} // namespace duckdb
