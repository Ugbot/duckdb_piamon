-- Create Parquet test data for Paimon extension testing

-- First, create a table with our test data
CREATE TABLE users_test (
    id INTEGER,
    name VARCHAR,
    age INTEGER,
    city VARCHAR
);

-- Insert test data
INSERT INTO users_test VALUES 
    (1, 'Alice', 25, 'New York'),
    (2, 'Bob', 30, 'San Francisco'),
    (3, 'Charlie', 35, 'Chicago'),
    (4, 'Diana', 28, 'Boston'),
    (5, 'Eve', 42, 'Seattle');

-- Export to Parquet format
COPY users_test TO 'paimon_data/users/data/test_users.parquet' (FORMAT 'parquet');

-- Verify the file was created
SELECT 'Parquet file created successfully' as status;

-- Test reading it back
SELECT * FROM read_parquet('paimon_data/users/data/test_users.parquet');

-- Clean up
DROP TABLE users_test;
