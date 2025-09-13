#include "storage/prc_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

PRCTransactionManager::PRCTransactionManager(AttachedDatabase &db_p, PRCCatalog &prc_catalog)
    : TransactionManager(db_p), prc_catalog(prc_catalog) {
}

Transaction &PRCTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<PRCTransaction>(prc_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData PRCTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &prc_transaction = transaction.Cast<PRCTransaction>();
	try {
		prc_transaction.Commit();
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void PRCTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &prc_transaction = transaction.Cast<PRCTransaction>();
	prc_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void PRCTransactionManager::Checkpoint(ClientContext &context, bool force) {
	auto &transaction = PRCTransaction::Get(context, db.GetCatalog());
	// auto &db = transaction.GetConnection();
	// db.Execute("CHECKPOINT");
}

} // namespace duckdb
