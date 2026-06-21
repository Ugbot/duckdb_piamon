#include "paimon_multi_file_reader.hpp"
#include "paimon_multi_file_list.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/multi_file_options.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

PaimonMultiFileReader::PaimonMultiFileReader(shared_ptr<TableFunctionInfo> function_info)
    : MultiFileReader(), function_info(std::move(function_info)) {
}

unique_ptr<MultiFileReader> PaimonMultiFileReader::CreateInstance(const TableFunction &table) {
	return make_uniq<PaimonMultiFileReader>(table.function_info);
}

shared_ptr<MultiFileList> PaimonMultiFileReader::CreateFileList(ClientContext &context, const vector<string> &paths,
                                                                const FileGlobInput &glob_input) {
	string table_location = paths.empty() ? "" : paths[0];
	return make_shared_ptr<PaimonMultiFileList>(context, table_location);
}

bool PaimonMultiFileReader::Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                                 vector<string> &names, MultiFileReaderBindData &bind_data) {
	auto &paimon_list = dynamic_cast<PaimonMultiFileList &>(files);

	// Bind the schema from Paimon metadata
	paimon_list.Bind(return_types, names, paimon_options);

	if (!return_types.empty()) {
		// We have the authoritative Paimon schema. Set up the column definitions and map data-file
		// columns to it by name (PK tables carry extra system columns — _KEY_*, _VALUE_KIND,
		// _SEQUENCE_NUMBER — which are simply not matched and therefore dropped here).
		// NOTE: we must NOT delegate to MultiFileReader::Bind afterwards — it would re-derive the
		// schema from the first data file and append it, producing duplicate columns.
		auto &columns = bind_data.schema;
		for (idx_t i = 0; i < names.size(); i++) {
			columns.push_back(MultiFileColumnDefinition(names[i], return_types[i]));
		}
		bind_data.mapping = MultiFileColumnMappingMode::BY_NAME;
		return true;
	}

	// No Paimon metadata schema available — let the base class infer from the data files.
	return MultiFileReader::Bind(options, files, return_types, names, bind_data);
}

void PaimonMultiFileReader::BindOptions(MultiFileOptions &options, MultiFileList &files,
                                        vector<LogicalType> &return_types, vector<string> &names,
                                        MultiFileReaderBindData &bind_data) {
	// Nothing extra needed for now
}

unique_ptr<MultiFileReaderGlobalState> PaimonMultiFileReader::InitializeGlobalState(
    ClientContext &context, const MultiFileOptions &file_options, const MultiFileReaderBindData &bind_data,
    const MultiFileList &file_list, const vector<MultiFileColumnDefinition> &global_columns,
    const vector<ColumnIndex> &global_column_ids) {
	vector<LogicalType> extra_columns;
	return make_uniq<PaimonMultiFileReaderGlobalState>(std::move(extra_columns), file_list);
}

void PaimonMultiFileReader::FinalizeBind(MultiFileReaderData &reader_data, const MultiFileOptions &file_options,
                                         const MultiFileReaderBindData &options,
                                         const vector<MultiFileColumnDefinition> &global_columns,
                                         const vector<ColumnIndex> &global_column_ids, ClientContext &context,
                                         optional_ptr<MultiFileReaderGlobalState> global_state) {
	// Let the base class handle column mapping
	MultiFileReader::FinalizeBind(reader_data, file_options, options, global_columns, global_column_ids, context,
	                              global_state);
}

void PaimonMultiFileReader::FinalizeChunk(ClientContext &context, const MultiFileBindData &bind_data,
                                          BaseFileReader &reader, const MultiFileReaderData &reader_data,
                                          DataChunk &input_chunk, DataChunk &output_chunk,
                                          ExpressionExecutor &executor,
                                          optional_ptr<MultiFileReaderGlobalState> global_state) {
	// Let the base class handle chunk finalization
	MultiFileReader::FinalizeChunk(context, bind_data, reader, reader_data, input_chunk, output_chunk, executor,
	                               global_state);
}

bool PaimonMultiFileReader::ParseOption(const string &key, const Value &val, MultiFileOptions &options,
                                        ClientContext &context) {
	auto loption = StringUtil::Lower(key);

	if (loption == "metadata_compression_codec") {
		paimon_options.metadata_compression_codec = StringValue::Get(val);
		return true;
	} else if (loption == "version") {
		paimon_options.table_version = StringValue::Get(val);
		return true;
	} else if (loption == "snapshot_from_timestamp") {
		if (paimon_options.snapshot_lookup.snapshot_source != PaimonOptions::SnapshotLookup::SnapshotSource::LATEST) {
			throw InvalidInputException("Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
		}
		paimon_options.snapshot_lookup.snapshot_source = PaimonOptions::SnapshotLookup::SnapshotSource::FROM_TIMESTAMP;
		paimon_options.snapshot_lookup.snapshot_timestamp = val.GetValue<timestamp_t>();
		return true;
	} else if (loption == "snapshot_from_id") {
		if (paimon_options.snapshot_lookup.snapshot_source != PaimonOptions::SnapshotLookup::SnapshotSource::LATEST) {
			throw InvalidInputException("Can't use 'snapshot_from_id' in combination with 'snapshot_from_timestamp'");
		}
		paimon_options.snapshot_lookup.snapshot_source = PaimonOptions::SnapshotLookup::SnapshotSource::FROM_ID;
		paimon_options.snapshot_lookup.snapshot_id = val.GetValue<uint64_t>();
		return true;
	}

	return false;
}

} // namespace duckdb
