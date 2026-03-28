#include "paimon_extension.hpp"
#include "paimon_functions.hpp"
#include "storage/paimon_catalog.hpp"
#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

namespace duckdb {

// Paimon filesystem-based storage extension
static unique_ptr<TransactionManager> CreatePaimonTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                     AttachedDatabase &db, Catalog &catalog) {
	return make_uniq<DuckTransactionManager>(db);
}

class PaimonStorageExtension : public StorageExtension {
public:
	PaimonStorageExtension() {
		attach = PaimonCatalog::Attach;
		create_transaction_manager = CreatePaimonTransactionManager;
	}
};

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();

	// Load required extensions
	ExtensionHelper::AutoLoadExtension(instance, "parquet");
	if (!instance.ExtensionIsLoaded("parquet")) {
		throw MissingExtensionException("The paimon extension requires the parquet extension to be loaded!");
	}

	ExtensionHelper::AutoLoadExtension(instance, "avro");
	if (!instance.ExtensionIsLoaded("avro")) {
		throw MissingExtensionException("The paimon extension requires the avro extension to be loaded!");
	}

	auto &config = DBConfig::GetConfig(instance);

	config.AddExtensionOption("unsafe_enable_version_guessing",
	                          "Enable globbing the filesystem to find the latest version metadata. "
	                          "This could result in reading an uncommitted version.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Register a simple test scalar function
	ScalarFunction test_func("paimon_test", {}, LogicalType::VARCHAR,
	                         [](DataChunk &args, ExpressionState &state, Vector &result) {
		                         result.SetValue(0, Value("Paimon extension loaded successfully!"));
	                         });
	loader.RegisterFunction(std::move(test_func));

	// Register Paimon table functions
	auto paimon_table_functions = PaimonFunctions::GetTableFunctions(loader);
	for (auto &fun : paimon_table_functions) {
		loader.RegisterFunction(std::move(fun));
	}

	// Register secrets for Paimon format
	try {
		SecretType paimon_secret_type;
		paimon_secret_type.name = "paimon";
		paimon_secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
		paimon_secret_type.default_provider = "config";
		loader.RegisterSecretType(paimon_secret_type);
	} catch (const std::exception &e) {
		// Continue even if secret registration fails
	}

	// Register storage extension for filesystem-based Paimon catalog
	try {
		config.storage_extensions["paimon"] = make_uniq<PaimonStorageExtension>();
	} catch (const std::exception &e) {
		// Continue even if storage extension registration fails
	}
}

void PaimonExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

string PaimonExtension::Name() {
	return "paimon";
}

} // namespace duckdb

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(paimon, loader) {
	duckdb::LoadInternal(loader);
}
}
