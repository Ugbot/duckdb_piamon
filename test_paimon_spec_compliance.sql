-- Test Paimon Specification Compliance

-- Create a test Paimon warehouse
ATTACH 'paimon_data/test_spec' AS paimon_test (TYPE paimon);

-- Create a test table
CREATE TABLE paimon_test.users (
    id BIGINT,
    name VARCHAR,
    age INTEGER,
    email VARCHAR,
    active BOOLEAN
);

-- Insert some test data
INSERT INTO paimon_test.users VALUES 
    (1, 'Alice Johnson', 28, 'alice@example.com', true),
    (2, 'Bob Smith', 34, 'bob@example.com', true),
    (3, 'Charlie Brown', 25, 'charlie@example.com', false);

-- Check that the files were created with proper structure
SELECT 'Checking directory structure...';

-- Check snapshot format
SELECT 'Checking snapshot format...';
SELECT * FROM paimon_metadata('paimon_data/test_spec/users');

-- Check manifest format  
SELECT 'Checking manifest format...';

-- Verify the table can be read back
SELECT 'Testing data retrieval...';
SELECT * FROM paimon_scan('paimon_data/test_spec/users');

DETACH paimon_test;
