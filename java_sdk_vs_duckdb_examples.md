# Java SDK vs DuckDB Extension Code Comparison

## Reading Data

### Java SDK ReadBuilder
```java
// Full ReadBuilder API
Table table = catalog.getTable(identifier);
TableReadBuilder readBuilder = table.readBuilder();

DataFileMeta[] files = readBuilder
    .withFilter(Equal.create("name", "Alice"))
    .withProjection(Arrays.asList("id", "name", "age"))
    .withPartitionFilter(Equal.create("city", "New York"))
    .withStreamScan(true)
    .withIncrementalBetween(100, 200)
    .plan()
    .files();

try (RecordReader<Record> reader = readBuilder.newRead().createReader(plan)) {
    reader.forEachRemaining(record -> {
        // Process records
    });
}
```

### DuckDB Extension (Current)
```sql
-- Basic table reading only
LOAD 'paimon.duckdb_extension';
SELECT id, name, age FROM paimon_scan('/path/to/table');
-- ❌ No filters, projections, streaming, or incremental reads
```

## Writing Data

### Java SDK WriteBuilder  
```java
// Full WriteBuilder API
TableWriteBuilder writeBuilder = table.writeBuilder();
TableCommit commit = writeBuilder
    .withExecutionMode(ExecutionMode.BATCH)
    .withOverwritePartition(PartitionSpec.of("city", "New York"))
    .newWrite()
    .write(GenericRecord.create(schema).setField("id", 1L))
    .compact(CompactOptions.builder().force(true).build())
    .commit();
```

### DuckDB Extension (Current)
```sql
-- Manual process - no native write API
CREATE TABLE temp_data AS SELECT 1 as id, 'Alice' as name;
COPY temp_data TO '/path/to/table/data/file.parquet' (FORMAT 'parquet');
-- ❌ No transactions, no batch operations, no compaction
```

## Schema Evolution

### Java SDK
```java
// Full schema evolution
SchemaManager schemaManager = table.schemaManager();
schemaManager
    .commitChanges()
    .addColumn("new_col", DataTypes.STRING())
    .dropColumn("old_col")
    .updateColumnType("age", DataTypes.BIGINT())
    .renameColumn("name", "full_name");
```

### DuckDB Extension (Current)
```sql
-- No schema evolution
SELECT 'Schema changes not supported' as limitation;
-- ❌ No ALTER TABLE operations
```

## Time Travel

### Java SDK
```java
// Time travel queries
Table table = catalog.getTable(identifier);
List<ManifestFileMeta> snapshots = table.snapshotManager().snapshots();

DataFileMeta[] files = table.readBuilder()
    .withSnapshot(snapshots.get(0).snapshotId())
    .withFilter("timestamp > '2023-01-01'")
    .plan()
    .files();
```

### DuckDB Extension (Current)
```sql
-- No time travel
SELECT snapshot_id FROM paimon_snapshots('/path/to/table');
-- Can see snapshots but cannot query historical data
-- ❌ No AS OF queries
```

## Streaming Reads

### Java SDK
```java
// Streaming reads with watermark
TableReadBuilder readBuilder = table.readBuilder();
readBuilder
    .withStreamScan(true)
    .withStreamScanStartUpMode(StreamScanStartupMode.LATEST)
    .withWatermark(new Watermark("event_time", 1000L));

try (StreamTableScan scan = readBuilder.newStreamScan()) {
    while (true) {
        List<Split> splits = scan.plan().splits();
        // Process streaming data
    }
}
```

### DuckDB Extension (Current)
```sql
-- No streaming support
SELECT * FROM paimon_scan('/path/to/table');
-- ❌ No real-time streaming capabilities
```

## Catalog Operations

### Java SDK
```java
// Rich catalog operations
Catalog catalog = CatalogFactory.createCatalog(options);
Table table = catalog.getTable(Identifier.create("db", "table"));

// Create table with partitioning
catalog.createTable(table, 
    Schema.builder()
        .column("id", DataTypes.BIGINT())
        .column("name", DataTypes.STRING())
        .partitionKeys("city")
        .build(),
    options);
```

### DuckDB Extension (Current)
```sql
-- Basic table creation only
SELECT * FROM paimon_create_table('/path/to/table', 
    '{"fields":[{"name":"id","type":"long"}]}');
-- ❌ No catalog management, no partitioning setup
```

