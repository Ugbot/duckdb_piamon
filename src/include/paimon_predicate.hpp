#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct PaimonPredicateStats {
	Value lower_bound;
	Value upper_bound;
	bool has_null = false;
	bool has_not_null = false;
};

struct PaimonPredicate {
public:
	PaimonPredicate() = delete;

public:
	// Main predicate pushdown function - determines if a filter can be pushed down
	static bool CanPushdownFilter(const TableFilter &filter, const PaimonPredicateStats &stats);

	// Parse filter expressions into Paimon-compatible predicates
	static vector<string> ParsePredicatesToStrings(const vector<unique_ptr<TableFilter>> &filters);

private:
	// Helper functions for different filter types
	static bool MatchBoundsConstantFilter(const TableFilter &filter, const PaimonPredicateStats &stats);
	static bool MatchBoundsConjunctionFilter(const TableFilter &filter, const PaimonPredicateStats &stats);
	static bool MatchBoundsNullFilter(const TableFilter &filter, const PaimonPredicateStats &stats);
};

} // namespace duckdb
