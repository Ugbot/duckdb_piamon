-- Test Catalog Integration for Paimon Extension
-- This script tests the new catalog integration functionality

-- Load the extension
LOAD '/Users/bengamble/duckdb_piamon/build/release/repository/v0.0.1/osx_arm64/paimon.duckdb_extension';

-- Test 1: Attach a Paimon warehouse
.echo Testing ATTACH DATABASE...
ATTACH DATABASE '/tmp/paimon_test_warehouse' AS paimon_db (TYPE paimon_fs);

-- Test 2: Check if tables are discovered
.echo Testing table discovery...
USE paimon_db;
SHOW TABLES;

-- Test 3: Try to describe a table
.echo Testing table description...
DESCRIBE users;

-- Test 4: Try to query a table
.echo Testing table query...
SELECT COUNT(*) FROM users;

-- Test 5: Try to create a new table
.echo Testing CREATE TABLE...
CREATE TABLE test_table (
    id INTEGER,
    name VARCHAR,
    value DOUBLE
);

-- Test 6: Check if new table appears
.echo Checking new table...
SHOW TABLES;

-- Test 7: Try to drop the table
.echo Testing DROP TABLE...
DROP TABLE test_table;

-- Test 8: Verify table was dropped
.echo Verifying table drop...
SHOW TABLES;

.echo Catalog integration test completed!
