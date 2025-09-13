//===----------------------------------------------------------------------===//
//                         DuckDB
//
// prc_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/irc_transaction.hpp"
#include "storage/prc_catalog.hpp"

namespace duckdb {

class PRCTransaction : public IRCTransaction {
public:
	PRCTransaction(PRCCatalog &prc_catalog, TransactionManager &manager, ClientContext &context);

	static PRCTransaction &Get(ClientContext &context, Catalog &catalog);

	// Paimon-specific transaction operations would go here
	// For now, inherit from IRCTransaction and adapt as needed

private:
	PRCCatalog &prc_catalog;
};

} // namespace duckdb
