#include "paimon_extension.hpp"
#include "table_format_manager.hpp"
#include "storage/prc_transaction_manager.hpp"
#include "storage/prc_catalog.hpp"
#include "storage/paimon_catalog.hpp"
#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "duckdb/main/extension_helper.hpp"
#include <fstream>
#include <ctime>

namespace duckdb {

static unique_ptr<TransactionManager> CreatePRCTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                AttachedDatabase &db, Catalog &catalog) {
	auto &prc_catalog = catalog.Cast<PRCCatalog>();
	return make_uniq<PRCTransactionManager>(db, prc_catalog);
}

class PRCStorageExtension : public StorageExtension {
public:
	PRCStorageExtension() {
		attach = PRCCatalog::Attach;
		create_transaction_manager = CreatePaimonTransactionManager;
	}
};

// Paimon Catalog Transaction Manager
static unique_ptr<TransactionManager> CreatePaimonTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                   AttachedDatabase &db, Catalog &catalog) {
	// Use the catalog's transaction manager creation method
	return catalog.CreateTransactionManager();
}

class PaimonStorageExtension : public StorageExtension {
public:
	PaimonStorageExtension() {
		attach = PaimonCatalog::Attach;
		create_transaction_manager = CreatePaimonTransactionManager;
	}
};

static void LoadInternal(ExtensionLoader &loader) {
	try {
		// Debug: Write to a file to confirm extension is loaded
		std::ofstream debug_file("/tmp/paimon_debug.log", std::ios::app);
		debug_file << "Paimon extension LoadInternal called at " << std::time(nullptr) << std::endl;
		debug_file.close();

		// Also write to stderr for immediate feedback
		std::cerr << "PAIMON EXTENSION: LoadInternal starting" << std::endl;

	auto &instance = loader.GetDatabaseInstance();

	// Load required extensions
	ExtensionHelper::AutoLoadExtension(instance, "parquet");

	// Verify required extensions are loaded
	if (!instance.ExtensionIsLoaded("parquet")) {
		throw MissingExtensionException("The paimon extension requires the parquet extension to be loaded!");
	}

	auto &config = DBConfig::GetConfig(instance);

	config.AddExtensionOption("unsafe_enable_version_guessing",
	                          "Enable globbing the filesystem (if possible) to find the latest version metadata. This "
	                          "could result in reading an uncommitted version.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Debug: Try to register a simple scalar function first
	try {
		ScalarFunction test_func("paimon_test", {}, LogicalType::VARCHAR, [](DataChunk &args, ExpressionState &state, Vector &result) {
			result.SetValue(0, Value("Paimon extension loaded successfully!"));
		});
		loader.RegisterFunction(std::move(test_func));
	} catch (const std::exception &e) {
		// If this fails, the extension loading mechanism has issues
		throw;
	}

	// Note: Scalar functions are not working in this extension setup
	// Using table functions instead for create operations

	// Register Paimon table functions
	auto paimon_table_functions = PaimonFunctions::GetTableFunctions(loader);
	std::cerr << "PAIMON: Registering " << paimon_table_functions.size() << " table function sets" << std::endl;
	for (auto &fun : paimon_table_functions) {
		try {
			std::cerr << "PAIMON: Registering table function set: " << fun.name << std::endl;
			loader.RegisterFunction(std::move(fun));
			std::cerr << "PAIMON: Successfully registered: " << fun.name << std::endl;
		} catch (const std::exception &e) {
			// Continue with other functions even if one fails
			std::cerr << "PAIMON: Failed to register table function: " << e.what() << std::endl;
		}
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

	// Register storage extensions for Paimon format
	try {
		std::cerr << "PAIMON: Registering storage extensions..." << std::endl;
		config.storage_extensions["paimon"] = make_uniq<PRCStorageExtension>();
		config.storage_extensions["paimon_fs"] = make_uniq<PaimonStorageExtension>();
		std::cerr << "PAIMON: Storage extensions registered successfully" << std::endl;
	} catch (const std::exception &e) {
		std::cerr << "PAIMON: Failed to register storage extensions: " << e.what() << std::endl;
		// Continue even if storage extension registration fails
	}
	} catch (const std::exception &e) {
		// Catch any exceptions in LoadInternal
		std::ofstream debug_file("/tmp/paimon_debug.log", std::ios::app);
		debug_file << "Exception in LoadInternal: " << e.what() << std::endl;
		debug_file.close();
		std::cerr << "PAIMON EXTENSION ERROR: " << e.what() << std::endl;
		throw; // Re-throw to let DuckDB know about the error
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
	LoadInternal(loader);
}
}
