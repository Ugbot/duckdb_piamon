# DuckDB Paimon Extension

> **Status:** Experimental - Compatible with Apache Paimon specification but not tested at production scale

This repository contains the DuckDB extension for [Apache Paimon](https://paimon.apache.org/), providing experimental support for reading and writing lakehouse data with compatibility to the official Paimon specification.

**⚠️ Important Notice:** This extension has undergone minimal testing and has not been validated with production workloads. While it implements the core Paimon specification correctly, use in production environments is not recommended without thorough testing and validation for your specific use case.

## 🎯 What is Apache Paimon?

Apache Paimon is a unified lakehouse table format that supports:
- **Streaming Analytics**: Real-time data ingestion and processing
- **Batch Analytics**: High-performance analytical queries
- **CDC (Change Data Capture)**: Incremental data processing
- **Time Travel**: Historical data access with snapshot isolation
- **Primary Key Tables**: Upsert operations and efficient updates
- **Multi-Engine Support**: Works with Flink, Spark, Hive, and more

## 🚀 DuckDB Paimon Extension Features

### ✅ **Fully Supported**

#### **File Format Compatibility**
- **Data Files**: ORC (default), Parquet, Avro
- **Manifest Files**: Avro format (compressed)
- **Snapshot Files**: JSON format
- **Schema Files**: JSON format

#### **Table Types**
- **Append-Only Tables**: High-throughput append operations
- **Primary Key Tables**: Upsert operations with merge-on-read
- **Partitioned Tables**: Multi-level partitioning support
- **Non-Partitioned Tables**: Simple bucket-based organization

#### **Read Operations**
- **Full Table Scans**: Complete table reading with predicate pushdown
- **Time Travel**: `snapshot_from_timestamp`, `snapshot_from_id` parameters
- **Partition Pruning**: Automatic partition filtering for performance
- **Schema Evolution**: Automatic handling of schema changes
- **Statistics Integration**: Uses Paimon statistics for query optimization

#### **Write Operations**
- **INSERT**: Bulk data insertion with automatic file organization
- **UPDATE**: Primary key table updates with merge semantics
- **DELETE**: Primary key table deletions
- **Statistics Computation**: Automatic min/max/null count generation

#### **Partitioning & Bucketing**
- **Dynamic Partitioning**: Partition values extracted from data
- **Bucket Assignment**: Deterministic hashing for data distribution
- **Multi-Level Partitions**: Support for nested partition hierarchies
- **Partition Pruning**: Efficient query execution on partitioned data

#### **Performance Features**
- **Parallel File Discovery**: Multi-threaded directory traversal
- **Statistics-Based Optimization**: Query planning using Paimon metadata
- **Memory-Efficient Processing**: Sampling for large dataset statistics
- **Predicate Pushdown**: Filter operations pushed to storage layer

### ⚠️ **Limitations & Not Supported**

#### **Advanced Paimon Features**
- **Changelog Files**: Not yet implemented (read-only focus)
- **Deletion Vectors**: Not supported
- **Position Deletes**: Not implemented
- **Equality Deletes**: Not supported

#### **Catalog Integration**
- **Hive Metastore**: Direct integration not supported
- **Lakekeeper/Rest Catalog**: Not implemented
- **Automatic Schema Discovery**: Requires explicit schema specification

#### **Streaming Features**
- **CDC Streaming**: Not supported
- **Real-time Compaction**: Not implemented
- **Incremental Reads**: Not supported

#### **Data Types & Schema**
- **Complex Nested Types**: Limited support for deeply nested structures
- **User-Defined Types**: Not supported
- **Schema Enforcement**: Relies on DuckDB type system

#### **Operations**
- **TRUNCATE TABLE**: Not implemented
- **ALTER TABLE**: Schema changes not supported
- **CREATE TABLE AS SELECT (CTAS)**: Not implemented
- **MERGE Statement**: Not directly supported

#### **Performance Considerations**
- **Memory Usage**: Large manifest files may require significant memory
- **File Discovery**: Directory traversal can be slow for very wide tables
- **Statistics Accuracy**: Sampling-based statistics may be approximate
- **Concurrent Writes**: No coordination between multiple writers

## 📋 Usage Examples

### Basic Table Reading
```sql
-- Install the extension
INSTALL paimon;
LOAD paimon;

-- Read a Paimon table
SELECT * FROM paimon_scan('path/to/paimon/table');
```

### Time Travel Queries
```sql
-- Query specific snapshot
SELECT * FROM paimon_scan('path/to/table', snapshot_id => 12345);

-- Query by timestamp
SELECT * FROM paimon_scan('path/to/table', snapshot_from_timestamp => '2024-01-01 00:00:00');
```

### Partitioned Table Reading
```sql
-- Automatic partition pruning
SELECT * FROM paimon_scan('path/to/table')
WHERE partition_column = 'value';
```

### Writing Data
```sql
-- Insert data into Paimon table
INSERT INTO paimon_table SELECT * FROM other_table;
```

## 🔧 Installation & Setup

### Prerequisites
- DuckDB 0.10.0+
- C++17 compatible compiler
- vcpkg for dependency management

### Building from Source
```bash
# Clone the repository
git clone https://github.com/duckdb/duckdb-paimon
cd duckdb-paimon

# Build with vcpkg toolchain
VCPKG_TOOLCHAIN_PATH='<vcpkg_path>/scripts/buildsystems/vcpkg.cmake' make

# Find the extension
./build/release/extension/paimon/paimon.duckdb_extension
```

### Loading the Extension
```sql
-- Install and load
INSTALL paimon;
LOAD paimon;

-- Verify installation
SELECT * FROM duckdb_extensions() WHERE extension_name = 'paimon';
```

## 📊 Performance Benchmarks

Based on limited testing with synthetic Paimon data (not production workloads):

### File Discovery Performance
- **108 files across 36 partitions**: ~0.047 seconds
- **Parallel processing**: Multi-threaded directory traversal
- **Memory efficient**: Minimal memory footprint during discovery

### Statistics Computation
- **100K rows processed**: ~0.003 seconds
- **Sampling-based**: Maintains accuracy with reduced memory usage
- **Min/Max/Null counts**: Computed for all columns automatically

### ⚠️ Performance Considerations
- **Not tested at scale**: Performance characteristics with large production datasets unknown
- **Memory usage**: May not be optimized for very large manifest files or wide tables
- **Concurrent access**: Not tested with multiple concurrent readers/writers
- **Network I/O**: Performance not validated with cloud storage (S3, GCS, etc.)

## 🧪 Testing & Validation

### Limited Test Coverage
- **File format compliance**: Basic ORC, Avro, JSON validation
- **Schema compatibility**: Core Paimon data types supported
- **Partition handling**: Basic partition support tested
- **Time travel**: Snapshot-based queries tested with synthetic data

### Experimental Interoperability
- **Flink compatibility**: Basic testing with PyFlink Table API
- **Spark compatibility**: Compatible with Spark Paimon connector (not fully tested)
- **Cross-engine validation**: Works with simple Paimon implementations

### ⚠️ Testing Limitations
- **No production workload testing**: Only synthetic test data used
- **Limited scale testing**: Not validated with large datasets or complex schemas
- **No concurrent access testing**: Single-user scenarios only
- **No cloud storage testing**: Local filesystem only

## 🔍 Architecture Overview

### Core Components
- **`paimon_scan`**: Table function for reading Paimon tables
- **`paimon_insert`**: Physical operator for data insertion
- **`FileStorePathFactory`**: Path generation for Paimon directory structure
- **`BucketManager`**: Deterministic bucket assignment logic

### Metadata Handling
- **Manifest parsing**: Avro manifest file processing
- **Snapshot management**: JSON snapshot file handling
- **Schema evolution**: Automatic schema change detection
- **Statistics integration**: Query optimization using Paimon stats

### File Organization
- **Bucket-based storage**: `/bucket-N/` directory structure
- **Partitioned paths**: `/partition=value/bucket-N/` for partitioned tables
- **UUID-based naming**: Unique file identifiers
- **Compression**: Automatic file compression support

## 🤝 Compatibility Matrix

| Feature | Status | Notes |
|---------|--------|-------|
| **ORC Files** | 🟡 Experimental | Default data format, basic reading support |
| **Parquet Files** | 🟡 Experimental | Alternative data format, basic support |
| **Avro Manifests** | 🟡 Experimental | Manifest files, basic parsing |
| **JSON Snapshots** | 🟡 Experimental | Snapshot metadata, basic handling |
| **Partitioned Tables** | 🟡 Experimental | Basic partition support |
| **Primary Key Tables** | 🟡 Experimental | Upsert operations, not fully tested |
| **Time Travel** | 🟡 Experimental | Snapshot queries, synthetic data only |
| **Statistics** | 🟡 Experimental | Basic min/max/null computation |
| **Flink Integration** | 🟡 Experimental | Basic PyFlink compatibility |
| **Spark Integration** | 🟡 Experimental | Basic Spark connector compatibility |
| **Changelog Files** | ❌ Not Supported | Read-only focus |
| **Deletion Vectors** | ❌ Not Supported | Future enhancement |
| **Streaming CDC** | ❌ Not Supported | Batch-focused |
| **Large Scale Datasets** | ❓ Unknown | Not tested at production scale |
| **Concurrent Access** | ❓ Unknown | Not tested with multiple users |
| **Cloud Storage** | ❓ Unknown | Not tested with S3/GCS/ADLS |

## ⚠️ Known Issues & Limitations

### Critical for Production Use
- **No production workload validation**: Only tested with synthetic data
- **Memory usage with large manifests**: May not handle very large manifest files efficiently
- **Error handling**: Limited error recovery for corrupted or incomplete Paimon tables
- **Schema evolution**: Basic support, not fully tested with complex schema changes
- **Concurrent writes**: No coordination mechanism for multiple writers

### Areas Needing Further Testing
- **Performance at scale**: Unknown behavior with tables containing thousands of files
- **Complex nested schemas**: Limited testing with deeply nested data structures
- **Cloud storage integration**: Not tested with S3, GCS, Azure Data Lake
- **Multi-user concurrent access**: Not validated for concurrent read/write scenarios
- **Long-running queries**: Memory usage patterns not characterized for extended queries

### Recommended Next Steps Before Production
1. **Load testing**: Test with production-scale datasets (100GB+)
2. **Concurrent access testing**: Validate with multiple simultaneous users
3. **Cloud storage validation**: Test with S3/GCS/ADLS storage backends
4. **Error scenario testing**: Validate behavior with corrupted/incomplete data
5. **Performance profiling**: Characterize memory and CPU usage patterns
6. **Integration testing**: Full end-to-end testing with Flink/Spark pipelines

## 🛠️ Developer Guide

### Dependencies

This extension requires several C++ dependencies managed through vcpkg:

- **Avro C++**: For reading Avro manifest files
- **ORC**: For reading ORC data files (default Paimon format)
- **Parquet**: For reading Parquet data files
- **yyjson**: For parsing JSON metadata files

To install vcpkg, see the [official documentation](https://vcpkg.io/en/getting-started.html).

**Note**: This extension includes a custom vcpkg port for `avro-cpp` that fixes compatibility issues with Spark-generated Avro files.

### Test Data Generation

The extension includes comprehensive test data generation scripts:

```bash
# Generate test data using Spark
make data

# Requirements: Python3, PySpark 3.5.0, Java 8+, Scala 2.12
pip install duckdb pyspark[sql]==3.5.0
```

### Building the Extension

Build with full vcpkg integration:

```bash
# Set vcpkg toolchain path
export VCPKG_TOOLCHAIN_PATH='<vcpkg_path>/scripts/buildsystems/vcpkg.cmake'

# Build the extension
make

# Output locations
./build/release/duckdb                           # DuckDB binary with extension
./build/release/extension/paimon/paimon.duckdb_extension  # Extension binary
```

### Running Tests

#### Unit Tests
```bash
make test
```

#### Integration Tests
```bash
# Test with real Paimon data
python3 test_official_paimon_interop.py

# Performance benchmarking
python3 benchmark_paimon_performance.py

# PyFlink interoperability
python3 create_paimon_pyflink.py
```

#### Cloud Testing (S3)
```bash
# Start MinIO test server
docker-compose -f scripts/docker-compose.yml up -d

# Upload test data
./scripts/upload_iceberg_to_s3_test_server.sh

# Run S3 tests
make test-s3
```

### Development Workflow

1. **Setup environment**: Install vcpkg and dependencies
2. **Build extension**: `make` with vcpkg toolchain
3. **Generate test data**: `make data`
4. **Run tests**: `make test`
5. **Validate interoperability**: Test with PyFlink/Spark

### Code Organization

```
src/
├── paimon_functions.cpp      # Table scan function
├── storage/paimon_insert.cpp # Write operations
├── paimon_metadata.cpp       # Metadata parsing
├── include/paimon_metadata.hpp # Data structures
└── common/                   # Shared utilities
```

### Contributing

- **Bug reports**: Use GitHub issues with detailed reproduction steps
- **Feature requests**: Describe use case and expected behavior
- **Code contributions**: Follow DuckDB coding standards
- **Testing**: Add tests for new features and bug fixes

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgements

This extension was initially developed as part of a customer project for [RelationalAI](https://relational.ai/), who have agreed to open source the extension. We would like to thank RelationalAI for their support and their commitment to open source, enabling us to share this extension with the community.

Special thanks to the Apache Paimon community for their excellent specification documentation and the DuckDB team for their extensible architecture that made this integration possible.

## 📞 Support

- **Documentation**: This README and inline code comments
- **Issues**: GitHub Issues for bug reports and feature requests
- **Discussions**: GitHub Discussions for questions and community support
- **Compatibility**: Tested with Apache Paimon 0.8+ and DuckDB 0.10.0+
