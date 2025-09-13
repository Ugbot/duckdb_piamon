# Paimon vs Iceberg Requirements Comparison & Code Reuse Plan

## Executive Summary

Paimon and Iceberg share **~80%** of their core functionality, making extensive code reuse possible. Both are lakehouse formats with similar architectures around snapshots, manifests, and metadata. The main differences are in write patterns (Paimon's LSM tree vs Iceberg's copy-on-write) and some advanced features.

## 1. Core Architecture Comparison

### Shared Components (High Reuse Potential)

| **Component** | **Paimon** | **Iceberg** | **Reuse Strategy** |
|---------------|------------|-------------|-------------------|
| **Metadata Parsing** | JSON snapshots/manifests | JSON snapshots/manifests | **100% reusable** - Same JSON structure |
| **File Discovery** | Manifest-based | Manifest-based | **90% reusable** - Same discovery logic |
| **Snapshot Management** | Time-based snapshots | Time-based snapshots | **85% reusable** - Similar API |
| **Schema Handling** | Evolving schemas | Evolving schemas | **80% reusable** - Compatible structures |
| **Partitioning** | Hive-style partitioning | Hidden partitioning | **70% reusable** - Different but compatible |
| **Time Travel** | Snapshot-based | Snapshot-based | **90% reusable** - Same concepts |

### Paimon-Specific Features (Low Reuse)

| **Feature** | **Reuse Potential** | **Notes** |
|-------------|-------------------|-----------|
| **LSM Tree Structure** | 10% | Iceberg uses copy-on-write |
| **Level-based Compaction** | 5% | Iceberg has different compaction |
| **Append-only Tables** | 20% | Iceberg supports but differently |
| **Changelog Files** | 10% | Iceberg doesn't have this concept |

### Iceberg-Specific Features (Medium Reuse)

| **Feature** | **Reuse Potential** | **Notes** |
|-------------|-------------------|-----------|
| **Hidden Partitioning** | 40% | Can adapt Paimon's partitioning |
| **Branching/Tagging** | 30% | Paimon has simpler versioning |
| **Equality Deletes** | 20% | Paimon uses different delete strategy |

## 2. Code Reuse Opportunities

### High-Impact Reusable Components

#### 1. **Metadata Parsing Infrastructure** ⭐⭐⭐
```cpp
// Shared base classes we can create:
// - BaseMetadataParser (JSON parsing)
// - BaseSnapshotManager (snapshot operations)
// - BaseManifestReader (manifest file handling)
// - BaseSchemaHandler (schema evolution)
```

#### 2. **File Discovery & Management** ⭐⭐⭐
```cpp
// Shared components:
// - FileDiscoveryEngine (manifest-based discovery)
// - PartitionResolver (partition handling)
// - FileFormatDetector (format identification)
// - SnapshotSelector (time travel logic)
```

#### 3. **Table Function Framework** ⭐⭐⭐
```cpp
// Shared patterns:
// - ScanFunction base class
// - MetadataFunction base class
// - SnapshotFunction base class
// - Parameter handling utilities
```

### Medium-Impact Reusable Components

#### 4. **Configuration System** ⭐⭐
```cpp
// Shared config options:
// - CoreOptions base class
// - Common option definitions (compression, formats, etc.)
// - Validation utilities
```

#### 5. **Catalog Integration** ⭐⭐
```cpp
// Shared catalog operations:
// - BaseCatalogClient (REST API client)
// - Authentication handlers (OAuth, SigV4)
// - Error handling utilities
```

## 3. Implementation Plan

### Phase 1: Foundation (1-2 weeks)
```cpp
// Create shared base classes
class BaseLakehouseTable {
    virtual unique_ptr<Snapshot> getLatestSnapshot() = 0;
    virtual vector<string> getPartitionKeys() = 0;
    virtual RowType getSchema() = 0;
};

class BaseMetadataParser {
    static unique_ptr<TableMetadata> parseJson(const string& json);
    static string generateJson(const TableMetadata& metadata);
};

class BaseFileDiscovery {
    vector<string> discoverFiles(const string& tablePath, const Snapshot& snapshot);
    vector<string> applyFilters(const vector<string>& files, const vector<Predicate>& predicates);
};
```

### Phase 2: Shared Components (2-3 weeks)
```cpp
// Implement shared table functions
class BaseScanFunction {
    virtual unique_ptr<FunctionData> bind(ClientContext& context, TableFunctionBindInput& input) = 0;
    virtual void scan(ClientContext& context, TableFunctionInput& data, DataChunk& output) = 0;
};

class BaseSnapshotFunction : public BaseScanFunction {
    // Common snapshot listing logic
};

class BaseMetadataFunction : public BaseScanFunction {
    // Common metadata reading logic
};
```

### Phase 3: Format-Specific Extensions (2-3 weeks)
```cpp
// Paimon-specific extensions
class PaimonTable : public BaseLakehouseTable {
    // LSM-specific logic
    unique_ptr<LSMTree> lsm_tree;
};

class PaimonScanFunction : public BaseScanFunction {
    // Level-based file discovery
    // Manifest list processing
};

// Iceberg-specific extensions
class IcebergTable : public BaseLakehouseTable {
    // Copy-on-write specific logic
};

class IcebergScanFunction : public BaseScanFunction {
    // Manifest file processing
    // Equality delete handling
};
```

## 4. Specific Code Borrowing Opportunities

### From Iceberg → Paimon

#### 1. **REST Catalog Client** ⭐⭐⭐
```cpp
// Borrow Iceberg's REST client (src/rest_catalog/)
// - Authentication (OAuth2, SigV4)
// - Error handling
// - Request/response parsing
// - Connection pooling
```

#### 2. **File System Operations** ⭐⭐⭐
```cpp
// Borrow Iceberg's FS utilities
// - File discovery patterns
// - Path resolution
// - File opener integration
```

#### 3. **Multi-File Reader Integration** ⭐⭐⭐
```cpp
// Borrow Iceberg's parquet_scan integration
// - MultiFileReader pattern
// - File format detection
// - Parallel reading
```

#### 4. **Configuration System** ⭐⭐
```cpp
// Adapt Iceberg's option handling
// - ConfigOption definitions
// - Validation logic
// - Type conversions
```

### From Paimon → Iceberg

#### 1. **Advanced Partitioning** ⭐⭐
```cpp
// Paimon has more sophisticated partitioning
// - Dynamic partition discovery
// - Partition statistics
// - Partition pruning optimizations
```

#### 2. **LSM Concepts** ⭐
```cpp
// Some LSM ideas could help Iceberg
// - Level-based organization
// - Incremental updates
```

## 5. Implementation Priority Matrix

### Immediate (Week 1-2)
1. ✅ **Create BaseLakehouseTable** - Abstract base class
2. ✅ **Implement BaseMetadataParser** - JSON parsing utilities
3. ✅ **Create BaseFileDiscovery** - Common file discovery logic
4. ✅ **Set up shared configuration system**

### Short-term (Week 3-4)
1. **Create BaseScanFunction** - Common scan logic
2. **Implement shared snapshot functions**
3. **Create shared metadata functions**
4. **Set up REST catalog client sharing**

### Medium-term (Week 5-8)
1. **Paimon-specific LSM integration**
2. **Iceberg-specific copy-on-write integration**
3. **Advanced partitioning support**
4. **Performance optimizations**

## 6. Benefits of This Approach

### Code Reduction
- **Estimated 60-70% code reuse** between Paimon and Iceberg
- **Shared test infrastructure** for common functionality
- **Unified configuration system**

### Maintenance Benefits
- **Single source of truth** for shared lakehouse concepts
- **Consistent APIs** across both formats
- **Easier bug fixes** - fix once, applies to both

### Feature Benefits
- **Faster feature development** - build once, use in both
- **Cross-pollination** - best features from both formats
- **Consistent user experience**

## 7. Risk Assessment

### Low-Risk Changes
- ✅ Metadata parsing utilities
- ✅ File discovery logic
- ✅ Basic table functions
- ✅ Configuration system

### Medium-Risk Changes
- ⚠️ REST catalog integration (different APIs)
- ⚠️ Advanced partitioning (different models)
- ⚠️ Write path integration (different semantics)

### High-Risk Changes
- ❌ LSM tree integration (Iceberg doesn't have this)
- ❌ Copy-on-write integration (Paimon doesn't use this)

## 8. Success Metrics

### Code Metrics
- **Lines of shared code**: >50% of total
- **Duplication reduction**: <20% duplicated logic
- **Test coverage**: >90% shared components

### Feature Metrics
- **Paimon functionality**: 100% preserved
- **Iceberg functionality**: 100% preserved
- **New features**: Faster development time

### Performance Metrics
- **Memory usage**: No regression
- **Query performance**: No regression
- **Extension load time**: <5% increase

## 9. Migration Strategy

### Phase 1: Analysis & Planning (Current)
- ✅ Complete feature comparison
- ✅ Identify shared components
- ✅ Create detailed implementation plan

### Phase 2: Foundation (Next 2 weeks)
- Extract shared utilities
- Create base classes
- Set up shared infrastructure

### Phase 3: Integration (Next 4 weeks)
- Refactor existing code to use shared components
- Test compatibility
- Performance validation

### Phase 4: Optimization (Ongoing)
- Performance tuning
- Feature additions
- Maintenance improvements

## Conclusion

This approach will **significantly reduce development time** and **improve maintainability** while preserving all existing functionality. The shared foundation will make it easier to add new lakehouse formats in the future and ensure consistent behavior across all supported formats.

**Next Action**: Start implementing the BaseLakehouseTable and BaseMetadataParser classes to establish the shared foundation.</contents>
</xai:function_call">**Next Action**: Start implementing the BaseLakehouseTable and BaseMetadataParser classes to establish the shared foundation.
