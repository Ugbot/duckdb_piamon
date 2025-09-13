//===----------------------------------------------------------------------===//
//                         DuckDB
//
// paimon_multi_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "paimon_metadata.hpp"

namespace duckdb {

struct PaimonMultiFileReaderGlobalState : public MultiFileReaderGlobalState {
public:
    PaimonMultiFileReaderGlobalState(vector<LogicalType> extra_columns_p, const MultiFileList &file_list_p)
        : MultiFileReaderGlobalState(std::move(extra_columns_p), file_list_p) {
    }
};

struct PaimonMultiFileReader : public MultiFileReader {
public:
    PaimonMultiFileReader(shared_ptr<TableFunctionInfo> function_info);

public:
    static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table);

public:
    shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
                                            const FileGlobInput &glob_input) override;
    bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types, vector<string> &names,
              MultiFileReaderBindData &bind_data) override;
    void BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                     vector<string> &names, MultiFileReaderBindData &bind_data) override;
    unique_ptr<MultiFileReaderGlobalState>
    InitializeGlobalState(ClientContext &context, const MultiFileOptions &file_options,
                          const MultiFileReaderBindData &bind_data, const MultiFileList &file_list,
                          const vector<MultiFileColumnDefinition> &global_columns,
                          const vector<ColumnIndex> &global_column_ids) override;
    void FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
                      const MultiFileReaderBindData &options, const vector<MultiFileColumnDefinition> &global_columns,
                      const vector<ColumnIndex> &global_column_ids, ClientContext &context,
                      optional_ptr<MultiFileReaderGlobalState> global_state) override;
    void FinalizeChunk(ClientContext &context, const MultiFileBindData &bind_data, BaseFileReader &reader,
                       const MultiFileReaderData &reader_data, DataChunk &input_chunk, DataChunk &output_chunk,
                       ExpressionExecutor &executor, optional_ptr<MultiFileReaderGlobalState> global_state) override;
    bool ParseOption(const string &key, const Value &val, MultiFileOptions &options, ClientContext &context) override;

private:
    string GetFileFormatFromPath(const string &file_path) const;

public:
    // Paimon-specific file discovery methods
    static vector<string> DiscoverPaimonFiles(ClientContext &context, const string &table_location);
    static vector<string> ApplyPartitionPruning(const vector<string> &files,
                                                const vector<unique_ptr<TableFilter>> &filters);

public:
    shared_ptr<TableFunctionInfo> function_info;
    PaimonOptions options;
    string file_format;
};

} // namespace duckdb
