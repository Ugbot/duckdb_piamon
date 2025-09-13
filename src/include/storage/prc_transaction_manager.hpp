//===----------------------------------------------------------------------===//
//                         DuckDB
//
// prc_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/prc_catalog.hpp"
#include "storage/prc_transaction.hpp"

namespace duckdb {

class PRCTransactionManager : public TransactionManager {
public:
	PRCTransactionManager(AttachedDatabase &db_p, PRCCatalog &prc_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	PRCCatalog &prc_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<PRCTransaction>> transactions;
};

} // namespace duckdb
