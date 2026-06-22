#include "paimon_binary_row.hpp"

#include "duckdb/common/types/decimal.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/date.hpp"

#include <cstring>

namespace duckdb {

namespace {

// Read a little-endian unsigned 64-bit value from an 8-byte slot.
uint64_t ReadLE64(const_data_ptr_t p) {
	uint64_t v = 0;
	for (idx_t i = 0; i < 8; i++) {
		v |= (uint64_t)p[i] << (i * 8);
	}
	return v;
}

int32_t ReadLE32(const_data_ptr_t p) {
	uint32_t v = 0;
	for (idx_t i = 0; i < 4; i++) {
		v |= (uint32_t)p[i] << (i * 8);
	}
	return (int32_t)v;
}

// Null-bit region width in bytes: round (arity + HEADER_SIZE_IN_BITS) up to whole 8-byte words.
idx_t NullBitsSizeInBytes(idx_t arity) {
	return ((arity + 63 + 8) / 64) * 8;
}

bool IsNullAt(const_data_ptr_t data, idx_t field) {
	// field i's null bit lives at bit position i + 8 (HEADER_SIZE_IN_BITS).
	idx_t bit = field + 8;
	return (data[bit / 8] & (1u << (bit % 8))) != 0;
}

// Extract the raw bytes of a variable-length field given its 8-byte slot value.
bool ReadVarBytes(const_data_ptr_t base, idx_t total_size, uint64_t slot, string &out) {
	// High bit of the most-significant byte (bit 63) marks an inline value.
	if (slot & (0x80ULL << 56)) {
		idx_t len = (idx_t)((slot >> 56) & 0x7F);
		if (len > 7) {
			return false;
		}
		out.resize(len);
		for (idx_t i = 0; i < len; i++) {
			out[i] = (char)((slot >> (i * 8)) & 0xFF);
		}
		return true;
	}
	// Otherwise the slot is a packed (offset << 32) | size pointing into the variable part. offset and
	// len are each masked to 32 bits; the comparison is written to avoid the offset+len addition (which,
	// though it can't overflow a 64-bit idx_t here, is fragile) — both must lie within total_size.
	idx_t offset = (idx_t)(slot >> 32);
	idx_t len = (idx_t)(slot & 0xFFFFFFFFULL);
	if (offset > total_size || len > total_size - offset) {
		return false;
	}
	out.assign((const char *)(base + offset), len);
	return true;
}

Value DecodeField(const_data_ptr_t base, idx_t total_size, idx_t null_bits, idx_t field,
                  const PaimonDataType &type) {
	auto logical = PaimonTypeToLogicalType(type);
	if (IsNullAt(base, field)) {
		return Value(logical);
	}
	// Precondition (guaranteed by Decode's up-front size check): the field's 8-byte slot is in bounds.
	D_ASSERT(null_bits + (field + 1) * 8 <= total_size);
	const_data_ptr_t slot = base + null_bits + field * 8;
	uint64_t raw = ReadLE64(slot);

	switch (type.type_root) {
	case PaimonTypeRoot::BOOLEAN:
		return Value::BOOLEAN(slot[0] != 0);
	case PaimonTypeRoot::INT:
		return Value::INTEGER(ReadLE32(slot));
	case PaimonTypeRoot::LONG:
		return Value::BIGINT((int64_t)raw);
	case PaimonTypeRoot::FLOAT: {
		uint32_t bits = (uint32_t)ReadLE32(slot);
		float f;
		memcpy(&f, &bits, sizeof(f));
		return Value::FLOAT(f);
	}
	case PaimonTypeRoot::DOUBLE: {
		double d;
		memcpy(&d, &raw, sizeof(d));
		return Value::DOUBLE(d);
	}
	case PaimonTypeRoot::DATE:
		return Value::DATE(date_t(ReadLE32(slot)));
	case PaimonTypeRoot::TIMESTAMP:
		// Compact timestamps (precision <= 3) store epoch-millis in the slot.
		return Value::TIMESTAMP(timestamp_t((int64_t)raw * 1000));
	case PaimonTypeRoot::DECIMAL: {
		int width = type.precision > 0 ? type.precision : 38;
		int scale = type.scale >= 0 ? type.scale : 0;
		if (width <= 18) {
			// Compact decimal: unscaled value as an 8-byte long in the slot.
			return Value::DECIMAL((int64_t)raw, (uint8_t)width, (uint8_t)scale);
		}
		// Larger decimals live in the variable part as a big-endian two's-complement integer.
		string bytes;
		if (!ReadVarBytes(base, total_size, raw, bytes) || bytes.empty()) {
			return Value(LogicalType::DECIMAL(width, scale));
		}
		// A DECIMAL(38) unscaled value fits in 16 bytes. A longer blob cannot be represented in
		// hugeint_t and would overflow the accumulation below (signed __int128 overflow is undefined
		// behaviour), so treat it as malformed and return NULL rather than risk UB.
		if (bytes.size() > 16) {
			return Value(LogicalType::DECIMAL(width, scale));
		}
		hugeint_t h(0);
		bool negative = (uint8_t)bytes[0] & 0x80;
		for (idx_t i = 0; i < bytes.size(); i++) {
			h = h * 256 + (uint8_t)bytes[i];
		}
		if (negative) {
			// Sign-extend: subtract 2^(8*len).
			hugeint_t base_pow(1);
			for (idx_t i = 0; i < bytes.size(); i++) {
				base_pow = base_pow * 256;
			}
			h = h - base_pow;
		}
		return Value::DECIMAL(h, (uint8_t)width, (uint8_t)scale);
	}
	case PaimonTypeRoot::STRING: {
		string s;
		if (!ReadVarBytes(base, total_size, raw, s)) {
			return Value(LogicalType::VARCHAR);
		}
		return Value(s);
	}
	case PaimonTypeRoot::BINARY: {
		string s;
		if (!ReadVarBytes(base, total_size, raw, s)) {
			return Value(LogicalType::BLOB);
		}
		return Value::BLOB((const_data_ptr_t)s.data(), s.size());
	}
	default:
		// Nested ARRAY/MAP/STRUCT inside a BinaryRow are not needed for partition/stat decoding.
		return Value(logical);
	}
}

} // namespace

vector<Value> PaimonBinaryRow::Decode(const_data_ptr_t data, idx_t size, const vector<PaimonDataType> &field_types) {
	vector<Value> result;
	idx_t arity = field_types.size();
	if (arity == 0) {
		return result;
	}
	// arity comes from the schema (trusted, small); this bound documents that null_bits + arity*8
	// below cannot overflow idx_t.
	D_ASSERT(arity <= (1ULL << 28));
	idx_t null_bits = NullBitsSizeInBytes(arity);
	if (size < null_bits + arity * 8) {
		// Malformed / truncated blob — return empty so the caller can fall back.
		return result;
	}
	result.reserve(arity);
	for (idx_t i = 0; i < arity; i++) {
		result.push_back(DecodeField(data, size, null_bits, i, field_types[i]));
	}
	return result;
}

vector<Value> PaimonBinaryRow::Decode(const string &blob, const vector<PaimonDataType> &field_types) {
	return Decode((const_data_ptr_t)blob.data(), blob.size(), field_types);
}

vector<Value> PaimonBinaryRow::DecodeSerialized(const string &blob, const vector<PaimonDataType> &field_types) {
	// Manifest BinaryRow blobs are prefixed with a 4-byte big-endian field count
	// (SerializationUtils.serializeBinaryRow). Skip it; the row's internal variable-part offsets are
	// relative to the start of the row that follows.
	if (blob.size() < 4) {
		return {};
	}
	return Decode((const_data_ptr_t)blob.data() + 4, blob.size() - 4, field_types);
}

} // namespace duckdb
