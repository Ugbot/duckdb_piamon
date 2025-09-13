-- Test Paimon extension loading and basic functionality
-- This script tests if the Paimon extension can be loaded and used

-- Try to load the extension
LOAD 'build/release/repository/v0.0.1/osx_arm64/paimon.duckdb_extension';

-- Check if it loaded
SELECT 'Paimon extension loaded successfully!' as status;

-- Try to see what functions are available
SELECT function_name
FROM duckdb_functions()
WHERE function_name LIKE '%paimon%'
ORDER BY function_name;

-- Try to see what table functions are available
SELECT name
FROM duckdb_table_functions()
WHERE name LIKE '%paimon%'
ORDER BY name;

-- Basic functionality test
SELECT 'Testing Paimon table format detection' as test;

-- Test if we can create a simple table (this won't work yet but shows the extension is loaded)
-- CREATE TABLE test_table (id INTEGER, name VARCHAR) USING paimon;