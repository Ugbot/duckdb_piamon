#include "storage/prc_catalog.hpp"
#include "storage/prc_transaction_manager.hpp"
#include "storage/paimon_catalog.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"

namespace duckdb {

PRCCatalog::PRCCatalog(AttachedDatabase &db_p, AccessMode access_mode, unique_ptr<IRCAuthorization> auth_handler,
                      IcebergAttachOptions &attach_options, const string &version)
    : IRCatalog(db_p, access_mode, std::move(auth_handler), attach_options, version) {
}

unique_ptr<Catalog> PRCCatalog::Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                      AttachedDatabase &db, const string &name, AttachInfo &info,
                                      AttachOptions &options) {
	std::cerr << "PRCCatalog::Attach called - path: " << info.path << ", db_type: " << options.db_type << std::endl;

	// Check if this is a filesystem-based Paimon warehouse
	// We use filesystem Paimon for local paths without explicit endpoints
	bool has_endpoint = false;
	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (lower_name == "endpoint") {
			has_endpoint = true;
			std::cerr << "PRCCatalog::Attach - found endpoint option" << std::endl;
			break;
		}
	}

	// Route to filesystem Paimon catalog if:
	// 1. No explicit endpoint provided, AND
	// 2. Path is not empty and not a URL
	if (!has_endpoint && !info.path.empty()) {
		if (!StringUtil::StartsWith(info.path, "http://") &&
		    !StringUtil::StartsWith(info.path, "https://") &&
		    !StringUtil::StartsWith(info.path, "s3://")) {
			std::cerr << "PRCCatalog::Attach - routing to PaimonCatalog for filesystem path: " << info.path << std::endl;
			// Use filesystem-based Paimon catalog
			return PaimonCatalog::Attach(storage_info, context, db, name, info, options);
		}
	}

	std::cerr << "PRCCatalog::Attach - using REST catalog path" << std::endl;

	IRCEndpointBuilder endpoint_builder;

	string endpoint_type_string;
	if (info.options.find("type") != info.options.end()) {
		endpoint_type_string = StringValue::Get(info.options["type"]);
	} else if (info.path.empty()) {
		throw InvalidInputException("Either a 'type' or path must be specified for Paimon catalog attachment");
	}

	// For REST catalog, reuse Iceberg attach logic but mark as Paimon
	auto result = IRCatalog::Attach(storage_info, context, db, name, info, options);

	// Cast to PRCCatalog if needed
	// For now, we use IRCatalog directly but could adapt for Paimon-specific features

	return result;
}

} // namespace duckdb
