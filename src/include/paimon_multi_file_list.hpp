#pragma once

#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "paimon_metadata.hpp"

namespace duckdb {

struct PaimonMultiFileList : public MultiFileList {
public:
	PaimonMultiFileList(ClientContext &context, const string &path);
	PaimonMultiFileList(ClientContext &context, const string &path, const vector<string> &files);

public:
	//! MultiFileList API
	vector<OpenFileInfo> GetAllFiles() override;
	FileExpandResult GetExpandResult() override;
	idx_t GetTotalFileCount() override;

protected:
	OpenFileInfo GetFile(idx_t i) override;

public:
	//! Paimon-specific methods
	void Bind(vector<LogicalType> &return_types, vector<string> &names, const PaimonOptions &options);

	//! Partition pruning methods
	unique_ptr<MultiFileList> DynamicFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                 const vector<string> &names, const vector<LogicalType> &types,
	                                                 const vector<column_t> &column_ids, TableFilterSet &filters) const;

private:
	//! Helper methods for partition pruning
	string ExtractPartitionValueFromPath(const string &file_path, const string &column_name) const;
	bool PartitionValueMatchesFilter(const string &partition_value, const LogicalType &type,
	                                 const TableFilter &filter) const;

public:
	ClientContext &context;
	string path;
	vector<string> files;
	unique_ptr<PaimonTableMetadata> metadata;
};

} // namespace duckdb
