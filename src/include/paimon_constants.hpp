//===----------------------------------------------------------------------===//
//                         DuckDB
//
// paimon_constants.hpp
//
// Single source of truth for Paimon's "magic" values — the row-kind ordinals, commit sentinels, and
// reserved name fragments that would otherwise appear as unexplained literals across the reader and
// writer. Spec-defined system column names (e.g. "_SEQUENCE_NUMBER") are used inline at their call
// sites: they are self-documenting and any drift is caught immediately by the cross-engine tests.
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace duckdb {
namespace paimon {

//! Paimon RowKind ordinals (org.apache.paimon.types.RowKind), stored in the _VALUE_KIND column of a
//! primary-key data file. After merge-on-read a key "exists" iff its latest row is an add (+I / +U).
enum class RowKind : int8_t {
	INSERT = 0,        // +I
	UPDATE_BEFORE = 1, // -U
	UPDATE_AFTER = 2,  // +U
	DELETE = 3,        // -D
};

//! SQL IN-list of the _VALUE_KIND values that survive merge-on-read (the "add" kinds: +I, +U).
static constexpr const char *VALUE_KIND_KEEP_SET = "(0, 2)";

//! A batch commit written by this extension has no Flink-style commit identifier or watermark; Paimon
//! uses Long.MAX_VALUE / Long.MIN_VALUE as the respective "absent" sentinels.
static constexpr int64_t COMMIT_IDENTIFIER_NONE = INT64_MAX;
static constexpr int64_t WATERMARK_NONE = INT64_MIN;

//! Per-key columns in a primary-key data file are named _KEY_<primary-key-column>.
static constexpr const char *KEY_COLUMN_PREFIX = "_KEY_";

//! Directory name Paimon uses for a null partition value (the partition.default-name default).
static constexpr const char *DEFAULT_PARTITION_NAME = "__DEFAULT_PARTITION__";

} // namespace paimon
} // namespace duckdb
