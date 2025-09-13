#include "table_format_manager.hpp"

namespace duckdb {

TableFormatManager::TableFormatManager() {
    // Register built-in formats
    RegisterFormat(make_uniq<IcebergTableFormat>());
    RegisterFormat(make_uniq<PaimonTableFormat>());
    // Note: IcebergTableFormat is not registered in Paimon-only builds
}

void TableFormatManager::RegisterFormat(unique_ptr<TableFormat> format) {
    formats_.push_back(std::move(format));
}

TableFormat* TableFormatManager::GetFormatForTable(const string &table_location) const {
    for (const auto& format : formats_) {
        if (format->CanHandleTable(table_location)) {
            return format.get();
        }
    }
    return nullptr;
}

} // namespace duckdb
