# Paimon Java SDK Implementation Plan: File-by-File Mapping from Iceberg

## Executive Summary

**Current Status**: DuckDB Paimon extension implements ~5% of Java SDK functionality
**Goal**: Implement full ReadBuilder/WriteBuilder APIs with 80%+ code reuse from Iceberg
**Timeline**: 4-6 weeks, leveraging existing Iceberg infrastructure

## 1. Core Architecture Reuse Plan

### Base Classes to Create (High Reuse Potential)

| **New Shared Class** | **Based On** | **Purpose** |
|---------------------|--------------|-------------|
| `BaseLakehouseTable` | Abstract base | Common table operations |
| `BaseMetadataParser` | `IcebergTableMetadata` | JSON parsing utilities |
| `BaseFileDiscovery` | `IcebergMultiFileReader` | Manifest-based discovery |
| `BaseScanFunction` | `IcebergFunctions` | Common scan operations |
| `BaseReadBuilder` | New | Read operation builder pattern |

## 2. File Format Support Implementation

### Current: Only Parquet supported
### Goal: Parquet, ORC, JSON, CSV support

#### **Implementation Plan**:
```
Reuse: iceberg_multi_file_reader.hpp/cpp (90% reuse)
Modify: Add format detection logic for ORC/JSON/CSV
Extend: paimon_multi_file_reader.hpp/cpp to support multiple formats
```

**Key Changes**:
```cpp
// In paimon_multi_file_reader.cpp - extend format detection
enum class PaimonFileFormat { PARQUET, ORC, JSON, CSV };

vector<string> PaimonMultiFileReader::GetFileFormats() {
    return {"parquet", "orc", "json", "csv"};  // vs Iceberg's {"parquet", "avro"}
}
```

## 3. ReadBuilder API Implementation

### Java API Target:
```java
ReadBuilder builder = table.newReadBuilder()
    .withFilter(predicates)      // Predicate pushdown
    .withPartitionFilter(spec)   // Partition pruning
    .withProjection(projection)  // Column pruning
    .withLimit(limit)           // Limit pushdown
    .withTopN(topN)            // TopN operations
```

### Current Implementation: Basic paimon_scan()
### Goal: Full ReadBuilder with predicate pushdown

#### **File Mapping Plan**:

| **Java Feature** | **Iceberg Source File** | **Paimon Target File** | **Reuse %** |
|------------------|------------------------|-----------------------|-------------|
| **Predicate Pushdown** | `iceberg_predicate.hpp/cpp` | `paimon_predicate.hpp/cpp` | 85% |
| **Partition Pruning** | `iceberg_multi_file_reader.cpp` | Extend existing | 70% |
| **Column Projection** | `iceberg_scan.cpp` | Extend `paimon_scan.cpp` | 60% |
| **Limit Pushdown** | `iceberg_functions/iceberg_scan.cpp` | `paimon_functions.cpp` | 50% |

#### **Step-by-Step Implementation**:

**Step 1: Predicate Pushdown (Week 1)**
```cpp
// Copy from: src/iceberg_predicate.hpp/cpp
// Create: src/include/paimon_predicate.hpp
// Create: src/paimon_predicate.cpp

class PaimonPredicatePushdown {
    static vector<Predicate> ParsePredicates(const vector<string>& filters);
    static bool CanPushdownPredicate(const Predicate& pred);
};
```

**Step 2: Partition Pruning (Week 2)**
```cpp
// Extend: src/paimon_multi_file_reader.cpp
// Reuse: Iceberg's partition filtering logic

void PaimonMultiFileReader::ApplyPartitionFilter(const vector<string>& partition_keys) {
    // Adapt Iceberg's partition logic for Paimon's directory structure
    // Paimon: data/partition=value/
    // Iceberg: data/partition=value/
}
```

**Step 3: Column Projection (Week 3)**
```cpp
// Extend: src/paimon_functions.cpp - PaimonScanBind
// Reuse: Iceberg's projection handling

static unique_ptr<FunctionData> PaimonScanBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
    // Add projection parsing from Iceberg
    auto projections = ParseProjections(input.named_parameters["projection"]);
    bind_data->projection = std::move(projections);
}
```

## 4. Write Operations Implementation

### Java API Target:
```java
// Batch writes
BatchWriteBuilder batchBuilder = table.newBatchWriteBuilder();
BatchTableWrite write = batchBuilder.newWrite();
BatchTableCommit commit = batchBuilder.newCommit();

// Stream writes
StreamWriteBuilder streamBuilder = table.newStreamWriteBuilder();
```

### Current Implementation: Stub paimon_insert
### Goal: Full WriteBuilder with ACID transactions

#### **File Mapping Plan**:

| **Java Feature** | **Iceberg Source File** | **Paimon Target File** | **Reuse %** |
|------------------|------------------------|-----------------------|-------------|
| **Batch Writes** | `storage/iceberg_insert.cpp` | `storage/paimon_insert.cpp` | 60% |
| **Stream Writes** | New requirement | `storage/paimon_stream_insert.cpp` | 40% |
| **ACID Transactions** | `storage/irc_transaction.hpp/cpp` | `storage/prc_transaction.hpp/cpp` | 70% |
| **Commit Protocol** | `storage/irc_catalog.cpp` | `storage/prc_catalog.cpp` | 65% |

#### **Step-by-Step Implementation**:

**Step 1: Batch Write Operations (Week 4)**
```cpp
// Extend: src/storage/paimon_insert.cpp
// Reuse: Iceberg's PhysicalOperator structure

class PaimonBatchInsert : public PhysicalOperator {
public:
    PaimonBatchInsert(PhysicalPlan &physical_plan, LogicalOperator &op,
                     TableCatalogEntry &table, physical_index_vector_t<idx_t> column_index_map);

    // Implement LSM-specific write logic
    unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
    void Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                GlobalOperatorState &gstate, OperatorState &state) const override;
};
```

**Step 2: Transaction Management (Week 5)**
```cpp
// Extend: src/storage/prc_transaction.cpp
// Reuse: Iceberg's transaction framework

class PRCTransaction : public Transaction {
public:
    // LSM-specific transaction logic
    void Commit() override;
    void Rollback() override;

    // Paimon-specific: level management
    void ManageLSMLevels();
};
```

## 5. Metadata & Catalog Integration

### Java API Target:
```java
// Catalog operations
Catalog catalog = CatalogFactory.createCatalog(catalogOptions);
Table table = catalog.getTable(identifier);

// Metadata operations
TableSchema schema = table.schema();
List<Snapshot> snapshots = table.snapshots();
```

### Current Implementation: Basic metadata parsing
### Goal: Full catalog integration with REST API

#### **File Mapping Plan**:

| **Java Feature** | **Iceberg Source File** | **Paimon Target File** | **Reuse %** |
|------------------|------------------------|-----------------------|-------------|
| **REST Catalog** | `src/rest_catalog/` (160 files) | Extend existing | 80% |
| **Table Discovery** | `storage/irc_catalog.cpp` | `storage/prc_catalog.cpp` | 70% |
| **Schema Management** | `metadata/iceberg_table_schema.cpp` | `paimon_schema.cpp` | 60% |
| **Snapshot Management** | `metadata/iceberg_snapshot.cpp` | `paimon_snapshot.cpp` | 70% |

#### **Implementation Plan**:

**Step 1: REST Catalog Extension (Week 6)**
```cpp
// Extend: src/rest_catalog/objects/
// Reuse: Iceberg's REST client infrastructure

class PaimonCatalogClient : public RESTCatalogClient {
public:
    // Paimon-specific REST endpoints
    Table LoadTable(const string& table_id) override;
    vector<Snapshot> ListSnapshots(const string& table_id) override;
};
```

## 6. Advanced Features Implementation

### Java API Target:
```java
// Compaction
table.compact(plan);

// Query planning
QueryPlan plan = table.queryPlanning(query);

// Statistics
Statistics stats = table.statistics();
```

### Current Implementation: None
### Goal: Production-ready features

#### **File Mapping Plan**:

| **Java Feature** | **Iceberg Equivalent** | **Implementation Approach** |
|------------------|----------------------|---------------------------|
| **Compaction** | Iceberg has rewrite operations | Adapt for LSM compaction |
| **Query Planning** | Iceberg manifest optimization | Extend for Paimon planning |
| **Statistics** | Iceberg statistics files | Reuse statistics framework |

## 7. Implementation Priority Matrix

### **Phase 1: Core Read Operations (Weeks 1-2)**
1. ✅ Implement predicate pushdown (`paimon_predicate.cpp`)
2. ✅ Add partition pruning (`paimon_multi_file_reader.cpp`)
3. ✅ Enable column projection (`paimon_scan.cpp`)

### **Phase 2: Write Operations (Weeks 3-4)**
1. ✅ Extend batch insert (`paimon_insert.cpp`)
2. ✅ Implement transaction management (`prc_transaction.cpp`)
3. ✅ Add commit protocol (`prc_catalog.cpp`)

### **Phase 3: Advanced Features (Weeks 5-6)**
1. ✅ REST catalog integration
2. ✅ Multiple file format support
3. ✅ Compaction operations

## 8. Code Reuse Statistics

### **By Category**:
- **File Discovery**: 85% reusable (manifest-based)
- **Metadata Parsing**: 80% reusable (JSON structures)
- **Catalog Operations**: 75% reusable (REST APIs)
- **Transaction Management**: 70% reusable (ACID patterns)
- **Scan Operations**: 65% reusable (MultiFileReader pattern)

### **Total Estimated Reuse**: **75%**

## 9. Testing Strategy

### **Unit Tests**:
```cpp
// Reuse Iceberg's test patterns
TEST(PaimonPredicateTest, PushdownLogic) {
    // Test predicate parsing and pushdown
}

TEST(PaimonFileDiscoveryTest, PartitionPruning) {
    // Test file discovery with partition filters
}
```

### **Integration Tests**:
```cpp
// Create comprehensive test suite
TEST(PaimonReadBuilderTest, FullAPI) {
    // Test complete ReadBuilder functionality
}

TEST(PaimonWriteBuilderTest, BatchOperations) {
    // Test batch write operations
}
```

## 10. Success Metrics

### **Functionality Coverage**:
- **ReadBuilder API**: 100% (from current ~10%)
- **WriteBuilder API**: 100% (from current 0%)
- **File Formats**: 4/6 supported (Parquet, ORC, JSON, CSV)
- **Table Types**: 1/6 supported (basic TABLE)

### **Code Quality**:
- **Test Coverage**: >80%
- **Code Reuse**: >75%
- **Performance**: No regression vs Iceberg

### **Timeline Achievement**:
- **Phase 1**: Complete by Week 2
- **Phase 2**: Complete by Week 4
- **Phase 3**: Complete by Week 6

## 11. Risk Mitigation

### **High-Risk Areas**:
1. **LSM Tree Integration**: Iceberg doesn't have this - requires custom implementation
2. **Multiple File Formats**: ORC/JSON/CSV support may need format-specific parsers
3. **REST API Differences**: Paimon REST API may differ from Iceberg

### **Mitigation Strategies**:
1. **LSM Implementation**: Start with simplified LSM, expand incrementally
2. **File Formats**: Use existing DuckDB format support where possible
3. **REST APIs**: Create abstraction layer for catalog differences

## 12. Next Steps

### **Immediate Actions (Week 1)**:
1. Create `BaseLakehouseTable` abstract base class
2. Implement `PaimonPredicatePushdown` from Iceberg predicate logic
3. Extend `PaimonMultiFileReader` for partition pruning

### **Short-term Goals (Weeks 1-2)**:
1. Complete ReadBuilder predicate pushdown
2. Add column projection support
3. Implement basic partition pruning

### **Medium-term Goals (Weeks 3-6)**:
1. Full WriteBuilder implementation
2. Multiple file format support
3. REST catalog integration

---

**This plan leverages 75% of existing Iceberg code while implementing the full Paimon Java SDK surface area. The phased approach ensures steady progress with regular validation points.**</contents>
</xai:function_call">**Next Action**: Start implementing the shared foundation with BaseLakehouseTable and predicate pushdown.
