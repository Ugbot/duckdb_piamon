# PAIMON CATALOG INTEGRATION PLAN

## Objective
Implement catalog integration for Paimon tables in DuckDB, enabling SQL DDL operations like:
- CREATE TABLE paimon_table (...)
- INSERT INTO paimon_table VALUES (...)  
- SELECT * FROM paimon_table

## Current State Analysis
- ✅ Table functions work (paimon_scan, paimon_create_table)
- ❌ No catalog integration - tables not registered in DuckDB catalog
- ❌ No SQL DDL support - cannot CREATE TABLE or INSERT directly

## Implementation Plan

### Phase 1: Basic Catalog Framework
1. Create PaimonCatalog class inheriting from Catalog
2. Implement StorageExtension pattern (like Iceberg)
3. Add ATTACH DATABASE support for Paimon warehouses
4. Register Paimon tables in catalog during attach

### Phase 2: DDL Operations  
5. Implement CREATE TABLE support
6. Implement DROP TABLE support
7. Add table entry management (PaimonTableEntry)

### Phase 3: DML Integration
8. Wire PaimonInsert into CREATE TABLE AS SELECT
9. Enable native INSERT INTO paimon_table
10. Add transaction support foundation

### Phase 4: Query Optimization
11. Integrate with DuckDB optimizer
12. Add partition pruning
13. Implement predicate pushdown

## Technical Architecture

### Catalog Structure
```
PaimonCatalog (inherits Catalog)
├── PaimonSchemaEntry (inherits SchemaCatalogEntry)
│   └── PaimonTableEntry (inherits TableCatalogEntry)
│       ├── PaimonTableInfo (table metadata)
│       └── PaimonInsertOperator (for writes)
```

### Storage Extension Pattern
- PaimonStorageExtension: StorageExtension
  - attach: PaimonCatalog::Attach()
  - create_transaction_manager: PaimonTransactionManager

### Table Entry Integration
- PaimonTableEntry provides:
  - GetStorageInfo() -> PaimonTableInfo
  - GetScanFunction() -> paimon_scan
  - GetInsertFunction() -> PaimonInsert

## Testing Strategy

### 1. Java SDK Test Warehouse Creation
Create a Java app that builds a reference Paimon warehouse with:
- Multiple tables with different schemas
- Partitioned and non-partitioned tables  
- Sample data in various scenarios

### 2. Catalog Attach Testing
Test ATTACH DATABASE commands:
```sql
ATTACH DATABASE 'file:///path/to/paimon/warehouse' AS paimon_db;
USE paimon_db;
SHOW TABLES;
DESCRIBE table1;
```

### 3. DDL Testing
Test SQL operations:
```sql
-- Read operations
SELECT * FROM table1;
SELECT * FROM table1 WHERE partition_col = 'value';

-- Write operations (future)
INSERT INTO table1 VALUES (...);
CREATE TABLE table2 AS SELECT * FROM table1;
```

## Success Criteria

### Functional Requirements
- ✅ ATTACH DATABASE works for Paimon warehouses
- ✅ SHOW TABLES lists Paimon tables  
- ✅ SELECT * FROM paimon_table works
- ✅ DESCRIBE paimon_table shows schema
- ✅ CREATE TABLE creates new Paimon tables
- ✅ DROP TABLE removes Paimon tables

### Performance Requirements
- ✅ Table discovery is fast (< 1s for typical warehouses)
- ✅ Schema inference works reliably
- ✅ No regression in existing functionality

### Compatibility Requirements
- ✅ Works with existing paimon_scan function
- ✅ Compatible with Java SDK created warehouses
- ✅ Follows Paimon specification
