#include "paimon_predicate.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

bool PaimonPredicate::CanPushdownFilter(const TableFilter &filter, const PaimonPredicateStats &stats) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON:
		return MatchBoundsConstantFilter(filter, stats);
	case TableFilterType::CONJUNCTION_AND:
	case TableFilterType::CONJUNCTION_OR:
		return MatchBoundsConjunctionFilter(filter, stats);
	case TableFilterType::IS_NULL:
	case TableFilterType::IS_NOT_NULL:
		return MatchBoundsNullFilter(filter, stats);
	case TableFilterType::EXPRESSION_FILTER: {
		auto &expression_filter = filter.Cast<ExpressionFilter>();
		auto &expr = *expression_filter.expr;
		if (expr.type == ExpressionType::OPERATOR_IS_NULL || expr.type == ExpressionType::OPERATOR_IS_NOT_NULL) {
			return MatchBoundsNullFilter(filter, stats);
		}
		// For complex expressions, be conservative
		return true;
	}
	default:
		// Conservative approach: don't push down unknown filter types
		return true;
	}
}

bool PaimonPredicate::MatchBoundsConstantFilter(const TableFilter &filter, const PaimonPredicateStats &stats) {
	auto &constant_filter = filter.Cast<ConstantFilter>();
	auto &constant_value = constant_filter.constant;

	// If we don't have bounds or the constant is null, we can't filter
	if (constant_value.IsNull() || stats.lower_bound.IsNull() || stats.upper_bound.IsNull()) {
		return true;
	}

	// Compare based on the filter type
	switch (constant_filter.comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		// For equality, check if constant is within bounds
		return constant_value >= stats.lower_bound && constant_value <= stats.upper_bound;
	case ExpressionType::COMPARE_GREATERTHAN:
		// Check if there's any value > constant in the bounds
		return stats.upper_bound > constant_value;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return stats.upper_bound >= constant_value;
	case ExpressionType::COMPARE_LESSTHAN:
		return stats.lower_bound < constant_value;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return stats.lower_bound <= constant_value;
	case ExpressionType::COMPARE_NOTEQUAL:
		// For not equal, we can only push down if the constant equals one of the bounds
		// and the bounds are equal (single value)
		if (stats.lower_bound == stats.upper_bound && constant_value == stats.lower_bound) {
			return false; // This would filter out all values
		}
		return true; // Conservative: don't push down
	default:
		return true; // Conservative: don't push down unknown comparisons
	}
}

bool PaimonPredicate::MatchBoundsConjunctionFilter(const TableFilter &filter, const PaimonPredicateStats &stats) {
	if (filter.filter_type == TableFilterType::CONJUNCTION_AND) {
		auto &conjunction_filter = filter.Cast<ConjunctionAndFilter>();
		// For AND, all child filters must be pushable
		for (auto &child : conjunction_filter.child_filters) {
			if (!CanPushdownFilter(*child, stats)) {
				return false;
			}
		}
		return true;
	} else if (filter.filter_type == TableFilterType::CONJUNCTION_OR) {
		auto &conjunction_filter = filter.Cast<ConjunctionOrFilter>();
		// For OR, if any child filter is not pushable, we can't push down the whole OR
		for (auto &child : conjunction_filter.child_filters) {
			if (!CanPushdownFilter(*child, stats)) {
				return false;
			}
		}
		return true;
	}
	return true;
}

bool PaimonPredicate::MatchBoundsNullFilter(const TableFilter &filter, const PaimonPredicateStats &stats) {
	if (filter.filter_type == TableFilterType::IS_NULL) {
		// Can push down IS NULL if we know there are nulls
		return stats.has_null;
	} else if (filter.filter_type == TableFilterType::IS_NOT_NULL) {
		// Can push down IS NOT NULL if we know there are non-null values
		return stats.has_not_null;
	}
	return true;
}

vector<string> PaimonPredicate::ParsePredicatesToStrings(const vector<unique_ptr<TableFilter>> &filters) {
	vector<string> result;

	for (auto &filter : filters) {
		if (!filter) continue;

		string filter_str;
		switch (filter->filter_type) {
		case TableFilterType::CONSTANT_COMPARISON: {
			auto &constant_filter = filter->Cast<ConstantFilter>();
			string col_name = "column"; // TODO: Get actual column name from context
			string op;
			switch (constant_filter.comparison_type) {
			case ExpressionType::COMPARE_EQUAL: op = "="; break;
			case ExpressionType::COMPARE_GREATERTHAN: op = ">"; break;
			case ExpressionType::COMPARE_GREATERTHANOREQUALTO: op = ">="; break;
			case ExpressionType::COMPARE_LESSTHAN: op = "<"; break;
			case ExpressionType::COMPARE_LESSTHANOREQUALTO: op = "<="; break;
			case ExpressionType::COMPARE_NOTEQUAL: op = "!="; break;
			default: op = "?"; break;
			}
			filter_str = StringUtil::Format("%s %s %s", col_name, op, constant_filter.constant.ToString());
			break;
		}
		case TableFilterType::IS_NULL: {
			string col_name = "column"; // TODO: Get actual column name from context
			filter_str = StringUtil::Format("%s IS NULL", col_name);
			break;
		}
		case TableFilterType::IS_NOT_NULL: {
			string col_name = "column"; // TODO: Get actual column name from context
			filter_str = StringUtil::Format("%s IS NOT NULL", col_name);
			break;
		}
		default:
			filter_str = "complex_filter";
			break;
		}
		result.push_back(filter_str);
	}

	return result;
}

} // namespace duckdb