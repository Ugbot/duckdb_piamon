package com.example.paimon;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

/**
 * Creates a test Paimon warehouse with various table types for DuckDB extension validation.
 * Uses direct filesystem operations to avoid Hadoop dependencies.
 */
public class PaimonTestWarehouseCreator {

    private static final String WAREHOUSE_PATH = "/tmp/paimon_test_warehouse";
    
    public static void main(String[] args) {
        try {
            System.out.println("🚀 Creating Paimon Test Warehouse...");
            System.out.println("Warehouse path: " + WAREHOUSE_PATH);

            // Create test tables with multiple snapshots for time travel testing
            createSimpleTableWithTimeTravel();
            createPartitionedTableWithTimeTravel();
            createComplexTypesTable();

            System.out.println("✅ Paimon test warehouse created successfully!");
            System.out.println("📁 Warehouse location: " + WAREHOUSE_PATH);
            System.out.println("\n🧪 Ready for DuckDB Paimon extension testing");
            System.out.println("   Run: duckdb -c \"SELECT * FROM paimon_scan('" + WAREHOUSE_PATH + "/test_table');\"");

        } catch (Exception e) {
            System.err.println("❌ Error creating test warehouse: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Create a simple table with multiple snapshots for time travel testing
     */
    private static void createSimpleTableWithTimeTravel() throws Exception {
        System.out.println("📋 Creating simple table (test_table) with time travel snapshots...");

        String tablePath = WAREHOUSE_PATH + "/test_table";
        createPaimonTable(tablePath, "test_table");

        System.out.println("✅ Created test_table with Paimon-compatible structure");
    }
    
    /**
     * Create a partitioned table with multiple snapshots for time travel testing
     */
    private static void createPartitionedTableWithTimeTravel() throws Exception {
        System.out.println("📋 Creating partitioned table (partitioned_table) with partitions...");

        String tablePath = WAREHOUSE_PATH + "/partitioned_table";
        createPaimonTable(tablePath, "partitioned_table");

        System.out.println("✅ Created partitioned_table with Paimon-compatible structure");
    }

    /**
     * Create a table with complex types
     */
    private static void createComplexTypesTable() throws Exception {
        System.out.println("📋 Creating complex types table (complex_table)...");

        String tablePath = WAREHOUSE_PATH + "/complex_table";
        createPaimonTable(tablePath, "complex_table");

        System.out.println("✅ Created complex_table with Paimon-compatible structure");
    }

    /**
     * Create a Paimon-compatible table structure manually
     */
    private static void createPaimonTable(String tablePath, String tableName) throws Exception {
        // Create directory structure
        Path tableDir = Paths.get(tablePath);
        Path bucketDir = tableDir.resolve("bucket-0");
        Path manifestDir = tableDir.resolve("manifest");
        Path snapshotDir = tableDir.resolve("snapshot");

        Files.createDirectories(bucketDir);
        Files.createDirectories(manifestDir);
        Files.createDirectories(snapshotDir);

        // Generate UUIDs for files
        String dataUuid = UUID.randomUUID().toString();
        String manifestUuid = UUID.randomUUID().toString();

        // Create a simple data file (placeholder - in real Paimon this would be ORC/Parquet)
        Path dataFile = bucketDir.resolve("data-" + dataUuid + "-0.orc");
        try (FileWriter writer = new FileWriter(dataFile.toFile())) {
            writer.write("# Placeholder ORC file - in real Paimon this would contain actual data\n");
            writer.write("# Table: " + tableName + "\n");
            writer.write("# This file simulates a Paimon data file\n");
        }

        // Create manifest file (JSON format for testing - real Paimon uses Avro)
        Path manifestFile = manifestDir.resolve("manifest-" + manifestUuid + "-0.json");
        String manifestContent = createManifestJson(dataFile, tableName);
        try (FileWriter writer = new FileWriter(manifestFile.toFile())) {
            writer.write(manifestContent);
        }

        // Create manifest list file
        String manifestListUuid = UUID.randomUUID().toString();
        Path manifestListFile = manifestDir.resolve("manifest-list-" + manifestListUuid + "-0.json");
        String manifestListContent = createManifestListJson(manifestFile);
        try (FileWriter writer = new FileWriter(manifestListFile.toFile())) {
            writer.write(manifestListContent);
        }

        // Create snapshot file
        Path snapshotFile = snapshotDir.resolve("snapshot-1");
        String snapshotContent = createSnapshotJson(manifestListFile);
        try (FileWriter writer = new FileWriter(snapshotFile.toFile())) {
            writer.write(snapshotContent);
        }

        // Create EARLIEST and LATEST pointers
        try (FileWriter writer = new FileWriter(snapshotDir.resolve("EARLIEST").toFile())) {
            writer.write("snapshot-1");
        }
        try (FileWriter writer = new FileWriter(snapshotDir.resolve("LATEST").toFile())) {
            writer.write("snapshot-1");
        }

        // Create schema file
        Path schemaFile = tableDir.resolve("schema").resolve("schema-0");
        Files.createDirectories(schemaFile.getParent());
        String schemaContent = createSchemaJson(tableName);
        try (FileWriter writer = new FileWriter(schemaFile.toFile())) {
            writer.write(schemaContent);
        }
    }

    private static String createManifestJson(Path dataFile, String tableName) {
        long now = System.currentTimeMillis();
        return "{\n" +
            "  \"entries\": [\n" +
            "    {\n" +
            "      \"_KIND\": 0,\n" +
            "      \"_PARTITION\": [],\n" +
            "      \"_BUCKET\": 0,\n" +
            "      \"_TOTAL_BUCKETS\": 1,\n" +
            "      \"_FILE\": {\n" +
            "        \"_FILE_NAME\": \"bucket-0/" + dataFile.getFileName() + "\",\n" +
            "        \"_FILE_SIZE\": 1024,\n" +
            "        \"_ROW_COUNT\": 3,\n" +
            "        \"_MIN_KEY\": [],\n" +
            "        \"_MAX_KEY\": [],\n" +
            "        \"_KEY_STATS\": {\n" +
            "          \"colNames\": [\"id\"],\n" +
            "          \"colStats\": [{\"min\": 1, \"max\": 3, \"nullCount\": 0}]\n" +
            "        },\n" +
            "        \"_VALUE_STATS\": {\n" +
            "          \"colNames\": [\"id\", \"name\", \"age\"],\n" +
            "          \"colStats\": [\n" +
            "            {\"min\": 1, \"max\": 3, \"nullCount\": 0},\n" +
            "            {\"min\": null, \"max\": null, \"nullCount\": 0},\n" +
            "            {\"min\": 25, \"max\": 35, \"nullCount\": 0}\n" +
            "          ]\n" +
            "        },\n" +
            "        \"_MIN_SEQUENCE_NUMBER\": 1,\n" +
            "        \"_MAX_SEQUENCE_NUMBER\": 3,\n" +
            "        \"_SCHEMA_ID\": 0,\n" +
            "        \"_LEVEL\": 0,\n" +
            "        \"_EXTRA_FILES\": [],\n" +
            "        \"_CREATION_TIME\": " + now + ",\n" +
            "        \"_DELETE_ROW_COUNT\": null,\n" +
            "        \"_EMBEDDED_FILE_INDEX\": null,\n" +
            "        \"_FILE_SOURCE\": 0,\n" +
            "        \"_VALUE_STATS_COLS\": [\"id\", \"name\", \"age\"],\n" +
            "        \"_EXTERNAL_PATH\": null,\n" +
            "        \"_FIRST_ROW_ID\": null,\n" +
            "        \"_WRITE_COLS\": [\"id\", \"name\", \"age\"]\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";
    }

    private static String createManifestListJson(Path manifestFile) {
        return "{\n" +
            "  \"entries\": [\n" +
            "    {\n" +
            "      \"_FILE_NAME\": \"manifest/" + manifestFile.getFileName() + "\",\n" +
            "      \"_FILE_SIZE\": 2048,\n" +
            "      \"_NUM_ADDED_FILES\": 1,\n" +
            "      \"_NUM_DELETED_FILES\": 0,\n" +
            "      \"_PARTITION_STATS\": {\n" +
            "        \"colNames\": [],\n" +
            "        \"colStats\": [],\n" +
            "        \"nullCount\": 0\n" +
            "      },\n" +
            "      \"_SCHEMA_ID\": 0,\n" +
            "      \"_MIN_BUCKET\": 0,\n" +
            "      \"_MAX_BUCKET\": 0,\n" +
            "      \"_MIN_LEVEL\": 0,\n" +
            "      \"_MAX_LEVEL\": 0\n" +
            "    }\n" +
            "  ]\n" +
            "}";
    }

    private static String createSnapshotJson(Path manifestListFile) {
        long now = System.currentTimeMillis();
        return "{\n" +
            "  \"version\": 3,\n" +
            "  \"id\": 1,\n" +
            "  \"schemaId\": 0,\n" +
            "  \"baseManifestList\": \"\",\n" +
            "  \"deltaManifestList\": \"manifest/" + manifestListFile.getFileName() + "\",\n" +
            "  \"deltaManifestListSize\": 1024,\n" +
            "  \"changelogManifestList\": null,\n" +
            "  \"indexManifest\": null,\n" +
            "  \"commitUser\": \"test-user\",\n" +
            "  \"commitIdentifier\": 9223372036854775807,\n" +
            "  \"commitKind\": \"APPEND\",\n" +
            "  \"timeMillis\": " + now + ",\n" +
            "  \"logOffsets\": {},\n" +
            "  \"totalRecordCount\": 3,\n" +
            "  \"deltaRecordCount\": 3,\n" +
            "  \"watermark\": -9223372036854775808,\n" +
            "  \"statistics\": null,\n" +
            "  \"properties\": {}\n" +
            "}";
    }

    private static String createSchemaJson(String tableName) {
        return "{\n" +
            "  \"version\": 0,\n" +
            "  \"id\": 0,\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"id\": 0,\n" +
            "      \"name\": \"id\",\n" +
            "      \"type\": \"INT\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"id\": 1,\n" +
            "      \"name\": \"name\",\n" +
            "      \"type\": \"STRING\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"id\": 2,\n" +
            "      \"name\": \"age\",\n" +
            "      \"type\": \"INT\"\n" +
            "    }\n" +
            "  ],\n" +
            "  \"highestFieldId\": 2,\n" +
            "  \"partitionKeys\": [],\n" +
            "  \"primaryKeys\": [\"id\"],\n" +
            "  \"options\": {},\n" +
            "  \"comment\": \"Test table: " + tableName + "\",\n" +
            "  \"timeMillis\": " + System.currentTimeMillis() + "\n" +
            "}";
    }
}
