#include "storage/prc_transaction.hpp"
#include "storage/prc_catalog.hpp"
#include "duckdb/transaction/transaction_manager.hpp"

namespace duckdb {

PRCTransaction::PRCTransaction(PRCCatalog &prc_catalog, TransactionManager &manager, ClientContext &context)
    : IRCTransaction(prc_catalog, manager, context), prc_catalog(prc_catalog) {
}

PRCTransaction &PRCTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<PRCTransaction>();
}

} // namespace duckdb
