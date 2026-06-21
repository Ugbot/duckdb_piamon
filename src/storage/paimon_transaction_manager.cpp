#include "storage/paimon_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

PaimonTransaction::PaimonTransaction(PaimonCatalog &paimon_catalog, TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context), paimon_catalog(paimon_catalog) {
}

PaimonTransaction::~PaimonTransaction() = default;

void PaimonTransaction::Start() {
}

void PaimonTransaction::Commit() {
}

void PaimonTransaction::Rollback() {
}

PaimonTransaction &PaimonTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<PaimonTransaction>();
}

PaimonTransactionManager::PaimonTransactionManager(AttachedDatabase &db_p, PaimonCatalog &paimon_catalog)
    : TransactionManager(db_p), paimon_catalog(paimon_catalog) {
}

Transaction &PaimonTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<PaimonTransaction>(paimon_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData PaimonTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &paimon_transaction = transaction.Cast<PaimonTransaction>();
	try {
		paimon_transaction.Commit();
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void PaimonTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &paimon_transaction = transaction.Cast<PaimonTransaction>();
	paimon_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void PaimonTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// No-op: Paimon state is persisted on commit as snapshot files; nothing to checkpoint.
}

} // namespace duckdb
