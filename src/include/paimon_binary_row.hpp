#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"
#include "paimon_metadata.hpp"

namespace duckdb {

//! Decoder for Paimon's BinaryRow encoding — the compact fixed-width row format used for the
//! _PARTITION, _MIN_KEY, _MAX_KEY fields and for SimpleStats _MIN_VALUES/_MAX_VALUES.
//!
//! Layout (see apache/paimon BinaryRow.java / BinarySection.java):
//!   [ null-bit region ][ fixed part: 8 bytes per field ][ variable-length part ]
//! - null-bit region size = ((arity + 63 + 8) / 64) * 8 bytes; field i's null bit is at bit i+8.
//! - each field occupies an 8-byte little-endian slot in the fixed part.
//! - variable-length values (string/binary/large decimal/...) are either inlined into the slot
//!   (length <= 7, high bit 0x80 of the most-significant byte set) or stored in the variable part
//!   with the slot holding a packed (offset<<32 | size).
class PaimonBinaryRow {
public:
	//! Decode a raw BinaryRow blob (no length prefix) into one Value per field, typed according to
	//! `field_types`. Null fields yield a NULL Value of the corresponding logical type. Returns an
	//! empty vector if the blob is too short / malformed.
	static vector<Value> Decode(const_data_ptr_t data, idx_t size, const vector<PaimonDataType> &field_types);

	//! Convenience overload for a raw BinaryRow held in a string/byte buffer.
	static vector<Value> Decode(const string &blob, const vector<PaimonDataType> &field_types);

	//! Decode a BinaryRow as serialized in manifests (_PARTITION / _MIN_KEY / _MAX_KEY / SimpleStats
	//! min/max). Paimon's SerializationUtils.serializeBinaryRow prepends a 4-byte big-endian field
	//! count, after which the row's variable-part offsets are relative to the row start. This strips
	//! that prefix and decodes the remainder.
	static vector<Value> DecodeSerialized(const string &blob, const vector<PaimonDataType> &field_types);
};

} // namespace duckdb
