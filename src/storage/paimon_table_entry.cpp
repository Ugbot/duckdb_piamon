#include "storage/paimon_table_entry.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

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
	throw InternalException("PaimonTableEntry::GetScanFunction called without entry lookup info");
}

TableFunction PaimonTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data,
                                                const EntryLookupInfo &lookup_info) {
	// Resolve the registered paimon_scan table function and bind it on this table's path.
	// This makes catalog-integrated reads (SELECT * FROM <attached>.<table>) go through the
	// same manifest-driven multi-file reader as paimon_scan('<path>').
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &catalog_schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	auto catalog_entry = catalog_schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, "paimon_scan");
	if (!catalog_entry) {
		throw InvalidInputException("Function with name \"paimon_scan\" not found!");
	}
	auto &paimon_scan_function_set = catalog_entry->Cast<TableFunctionCatalogEntry>();
	auto paimon_scan_function = paimon_scan_function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});

	named_parameter_map_t param_map;
	vector<LogicalType> return_types;
	vector<string> names;
	TableFunctionRef empty_ref;

	vector<Value> inputs = {Value(table_path)};
	TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, paimon_scan_function,
	                                  empty_ref);
	bind_data = paimon_scan_function.bind(context, bind_input, return_types, names);
	return paimon_scan_function;
}

TableStorageInfo PaimonTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	result.cardinality = 0;
	return result;
}

} // namespace duckdb
