-- Create Paimon catalog
CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'file:///tmp/real_flink_paimon_test'
);

-- Use the catalog
USE CATALOG paimon_catalog;

-- Create a table
CREATE TABLE users (
    id INT,
    name STRING,
    age INT,
    email STRING,
    active BOOLEAN,
    PRIMARY KEY (id) NOT ENFORCED
);

-- Insert test data
INSERT INTO users VALUES
(1, 'Alice Johnson', 28, 'alice@example.com', true),
(2, 'Bob Smith', 34, 'bob@example.com', true),
(3, 'Charlie Brown', 25, 'charlie@example.com', false),
(4, 'Diana Wilson', 42, 'diana@example.com', true),
(5, 'Edward Davis', 31, 'edward@example.com', true);
