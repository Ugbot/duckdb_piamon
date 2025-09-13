//===----------------------------------------------------------------------===//
//                         DuckDB
//
// paimon_extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class PaimonExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	string Name() override;
};

} // namespace duckdb
