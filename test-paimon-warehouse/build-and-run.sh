#!/bin/bash

set -e

echo "🔨 Building Paimon Test Warehouse Creator..."

# Build the application
mvn clean package

echo "🚀 Running Paimon Test Warehouse Creator..."

# Run the application
java -jar target/paimon-test-warehouse-1.0.0.jar

echo "✅ Test warehouse created successfully!"
echo "📁 Location: /tmp/paimon_test_warehouse"
echo ""
echo "🧪 Test with DuckDB extension:"
echo "SELECT * FROM paimon_scan('/tmp/paimon_test_warehouse/users');"
echo "SELECT * FROM paimon_scan('/tmp/paimon_test_warehouse/orders');"
echo "SELECT * FROM paimon_scan('/tmp/paimon_test_warehouse/products');"
