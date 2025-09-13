package com.example.paimon;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Creates a test Paimon warehouse with various table types for DuckDB extension validation.
 */
public class PaimonTestWarehouseCreator {

    private static final String WAREHOUSE_PATH = "file:///tmp/paimon_test_warehouse";
    
    public static void main(String[] args) {
        try {
            System.out.println("🚀 Creating Paimon Test Warehouse...");
            System.out.println("Warehouse path: " + WAREHOUSE_PATH);
            
            // Create filesystem catalog
            Catalog catalog = createFilesystemCatalog();
            
            // Create test tables
            createSimpleTable(catalog);
            createPartitionedTable(catalog);
            createComplexTypesTable(catalog);
            
            System.out.println("✅ Paimon test warehouse created successfully!");
            System.out.println("📁 Warehouse location: /tmp/paimon_test_warehouse");
            
        } catch (Exception e) {
            System.err.println("❌ Error creating test warehouse: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    private static Catalog createFilesystemCatalog() {
        Map<String, String> options = new HashMap<>();
        options.put("warehouse", WAREHOUSE_PATH);
        
        CatalogContext context = CatalogContext.create(options);
        return CatalogFactory.createCatalog(context);
    }
    
    /**
     * Create a simple table with basic types
     */
    private static void createSimpleTable(Catalog catalog) throws Exception {
        System.out.println("📋 Creating simple table (users)...");
        
        Schema schema = Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .column("age", DataTypes.INT())
            .column("email", DataTypes.STRING())
            .column("active", DataTypes.BOOLEAN())
            .primaryKey("id")
            .build();
            
        catalog.createTable("users", schema, false);
        
        // Insert sample data
        Table table = catalog.getTable("users");
        try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite();
             BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            
            // Sample user data
            Object[][] users = {
                {1, "Alice Johnson", 28, "alice@example.com", true},
                {2, "Bob Smith", 34, "bob@example.com", true},
                {3, "Charlie Brown", 25, "charlie@example.com", false},
                {4, "Diana Wilson", 42, "diana@example.com", true},
                {5, "Edward Davis", 31, "edward@example.com", true}
            };
            
            for (Object[] user : users) {
                InternalRow row = GenericRow.of(
                    user[0], user[1], user[2], user[3], user[4]
                );
                write.write(row);
            }
            
            List<CommitMessage> messages = write.prepareCommit();
            commit.commit(messages);
        }
        
        System.out.println("✅ Simple table created with 5 rows");
    }
    
    /**
     * Create a partitioned table
     */
    private static void createPartitionedTable(Catalog catalog) throws Exception {
        System.out.println("📋 Creating partitioned table (orders)...");
        
        Schema schema = Schema.newBuilder()
            .column("order_id", DataTypes.INT())
            .column("customer_id", DataTypes.INT())
            .column("product", DataTypes.STRING())
            .column("quantity", DataTypes.INT())
            .column("price", DataTypes.DOUBLE())
            .column("order_date", DataTypes.STRING()) // Will be partitioned by this
            .primaryKey("order_id")
            .partitionKeys("order_date")
            .build();
            
        catalog.createTable("orders", schema, false);
        
        // Insert sample data with different dates
        Table table = catalog.getTable("orders");
        try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite();
             BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            
            // Sample order data
            Object[][] orders = {
                {1001, 1, "Laptop", 1, 1299.99, "2024-01-15"},
                {1002, 2, "Mouse", 2, 49.98, "2024-01-15"},
                {1003, 3, "Keyboard", 1, 89.99, "2024-01-16"},
                {1004, 1, "Monitor", 1, 299.99, "2024-01-16"},
                {1005, 4, "Headphones", 1, 149.99, "2024-01-17"},
                {1006, 2, "Webcam", 1, 79.99, "2024-01-17"}
            };
            
            for (Object[] order : orders) {
                InternalRow row = GenericRow.of(
                    order[0], order[1], order[2], order[3], order[4], order[5]
                );
                write.write(row);
            }
            
            List<CommitMessage> messages = write.prepareCommit();
            commit.commit(messages);
        }
        
        System.out.println("✅ Partitioned table created with 6 rows across 3 partitions");
    }
    
    /**
     * Create a table with complex types
     */
    private static void createComplexTypesTable(Catalog catalog) throws Exception {
        System.out.println("📋 Creating complex types table (products)...");
        
        Schema schema = Schema.newBuilder()
            .column("product_id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .column("tags", DataTypes.ARRAY(DataTypes.STRING()))
            .column("specifications", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))
            .column("metadata", DataTypes.ROW(
                DataTypes.FIELD("created_at", DataTypes.STRING()),
                DataTypes.FIELD("updated_at", DataTypes.STRING()),
                DataTypes.FIELD("version", DataTypes.INT())
            ))
            .primaryKey("product_id")
            .build();
            
        catalog.createTable("products", schema, false);
        
        // Insert sample data with complex types
        Table table = catalog.getTable("products");
        try (BatchTableWrite write = table.newBatchWriteBuilder().newWrite();
             BatchTableCommit commit = table.newBatchWriteBuilder().newCommit()) {
            
            // Sample product data
            Object[][] products = {
                {2001, "Gaming Laptop", 
                 Arrays.asList("gaming", "laptop", "high-performance"),
                 Map.of("cpu", "Intel i7", "ram", "16GB", "storage", "512GB SSD"),
                 GenericRow.of("2024-01-01", "2024-01-15", 2)
                },
                {2002, "Wireless Mouse",
                 Arrays.asList("wireless", "mouse", "ergonomic"),
                 Map.of("connectivity", "Bluetooth", "battery", "2 years", "dpi", "1600"),
                 GenericRow.of("2024-01-02", "2024-01-10", 1)
                }
            };
            
            for (Object[] product : products) {
                InternalRow row = GenericRow.of(
                    product[0], product[1], product[2], product[3], product[4]
                );
                write.write(row);
            }
            
            List<CommitMessage> messages = write.prepareCommit();
            commit.commit(messages);
        }
        
        System.out.println("✅ Complex types table created with 2 rows");
    }
}
