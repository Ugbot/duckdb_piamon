//===----------------------------------------------------------------------===//
//                         DuckDB
//
// prc_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/irc_catalog.hpp"
#include "paimon_metadata.hpp"

namespace duckdb {

class PRCCatalog : public IRCatalog {
public:
	PRCCatalog(AttachedDatabase &db_p, AccessMode access_mode, unique_ptr<IRCAuthorization> auth_handler,
	          IcebergAttachOptions &attach_options, const string &version);

	static unique_ptr<Catalog> Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
	                                 AttachedDatabase &db, const string &name, AttachInfo &info,
	                                 AttachOptions &options);

	// Paimon-specific catalog operations would go here
	// For now, inherit from IRCatalog and adapt as needed
};

} // namespace duckdb
