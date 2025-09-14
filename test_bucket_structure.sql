-- Test Paimon bucket-based structure and Avro manifests

-- Create a test database
ATTACH 'test_paimon_bucket.db' AS paimon_test (TYPE paimon);

-- Create a test table (this should create the bucket structure)
CREATE TABLE paimon_test.users (
    id INTEGER,
    name VARCHAR,
    age INTEGER
);

-- Insert some test data (this should create bucket-0/ directory with ORC files)
INSERT INTO paimon_test.users VALUES 
    (1, 'Alice', 25),
    (2, 'Bob', 30),
    (3, 'Charlie', 35);

-- Check if the table was created successfully
SELECT 'Table created successfully';

-- Try to read the data back
SELECT * FROM paimon_test.users;

DETACH paimon_test;
