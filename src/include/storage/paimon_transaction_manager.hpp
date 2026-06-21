#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "storage/paimon_catalog.hpp"

namespace duckdb {

//! Minimal transaction for the filesystem-backed Paimon catalog. Paimon commits are realized as
//! atomic snapshot writes at INSERT/UPDATE/DELETE time (see paimon_insert.cpp), so the transaction
//! object itself carries no buffered state for now.
class PaimonTransaction : public Transaction {
public:
	PaimonTransaction(PaimonCatalog &paimon_catalog, TransactionManager &manager, ClientContext &context);
	~PaimonTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	static PaimonTransaction &Get(ClientContext &context, Catalog &catalog);

private:
	PaimonCatalog &paimon_catalog;
};

class PaimonTransactionManager : public TransactionManager {
public:
	PaimonTransactionManager(AttachedDatabase &db_p, PaimonCatalog &paimon_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	PaimonCatalog &paimon_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<PaimonTransaction>> transactions;
};

} // namespace duckdb
