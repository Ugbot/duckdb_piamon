# Paimon Test Warehouse Creator

This Java application creates a test Paimon warehouse with various table types for validating the DuckDB Paimon extension.

## Tables Created

### 1. `users` - Simple Table
- **Schema**: id (INT), name (STRING), age (INT), email (STRING), active (BOOLEAN)
- **Primary Key**: id
- **Data**: 5 user records

### 2. `orders` - Partitioned Table  
- **Schema**: order_id (INT), customer_id (INT), product (STRING), quantity (INT), price (DOUBLE), order_date (STRING)
- **Primary Key**: order_id
- **Partitions**: order_date
- **Data**: 6 orders across 3 date partitions

### 3. `products` - Complex Types Table
- **Schema**: 
  - product_id (INT)
  - name (STRING) 
  - tags (ARRAY<STRING>)
  - specifications (MAP<STRING, STRING>)
  - metadata (ROW<created_at, updated_at, version>)
- **Primary Key**: product_id
- **Data**: 2 products with complex nested data

## Usage

### Prerequisites
- Java 11 or higher
- Maven 3.6+

### Build the Application
```bash
mvn clean package
```

### Run the Application
```bash
java -jar target/paimon-test-warehouse-1.0.0.jar
```

Or using Maven:
```bash
mvn exec:java
```

## Output

The application creates a Paimon warehouse at `/tmp/paimon_test_warehouse` with the following structure:

```
/tmp/paimon_test_warehouse/
├── users/
│   ├── data/
│   │   └── data-*.parquet
│   ├── schema/
│   ├── snapshot/
│   └── manifest/
├── orders/
│   ├── data/
│   │   └── order_date=2024-01-15/
│   │   └── order_date=2024-01-16/
│   │   └── order_date=2024-01-17/
│   ├── schema/
│   ├── snapshot/
│   └── manifest/
└── products/
    ├── data/
    ├── schema/
    ├── snapshot/
    └── manifest/
```

## Testing with DuckDB Extension

After creating the warehouse, test it with the DuckDB Paimon extension:

```sql
-- Load the extension
LOAD '/path/to/paimon.duckdb_extension';

-- Scan the tables
SELECT * FROM paimon_scan('/tmp/paimon_test_warehouse/users');
SELECT * FROM paimon_scan('/tmp/paimon_test_warehouse/orders');
SELECT * FROM paimon_scan('/tmp/paimon_test_warehouse/products');
```

## Future Catalog Integration Testing

Once catalog integration is implemented, you should be able to:

```sql
-- Attach the warehouse (future feature)
ATTACH DATABASE '/tmp/paimon_test_warehouse' AS paimon_db;
USE paimon_db;

-- List tables
SHOW TABLES;

-- Query tables directly
SELECT * FROM users;
SELECT * FROM orders WHERE order_date = '2024-01-15';
DESCRIBE products;
```

## Troubleshooting

### Common Issues

1. **Permission Denied**: Make sure `/tmp` is writable
2. **Hadoop Dependencies**: Ensure Hadoop native libraries are available
3. **Port Conflicts**: If using REST catalog, ensure ports are available

### Logs

The application uses SLF4J for logging. Set logging level:

```bash
java -Dorg.slf4j.simpleLogger.defaultLogLevel=DEBUG -jar target/paimon-test-warehouse-1.0.0.jar
```
