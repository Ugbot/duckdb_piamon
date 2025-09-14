#!/usr/bin/env python3
"""
Create a real Paimon table using PyFlink Table API
This will test our DuckDB extension against actual Paimon data created by Flink
"""

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table import DataTypes
from pyflink.table.descriptors import Schema, OldCsv, FileSystem
from pyflink.table.types import RowType
import os
import urllib.request

def create_real_paimon_table():
    """Create a Paimon table using PyFlink and insert real data"""

    print("🚀 Creating REAL Paimon table using PyFlink Table API...")

    # Download Paimon connector if not present
    paimon_jar = "paimon-flink-1.18-0.8.0.jar"
    if not os.path.exists(paimon_jar):
        print("📥 Downloading Paimon connector...")
        url = "https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.18/0.8.0/paimon-flink-1.18-0.8.0.jar"
        urllib.request.urlretrieve(url, paimon_jar)
        print("✅ Paimon connector downloaded")

    # Create a batch environment (simpler for table operations)
    env_settings = EnvironmentSettings.in_batch_mode()
    table_env = TableEnvironment.create(env_settings)

    # Add the Paimon connector JAR to the classpath
    import pyflink
    # Note: In a real setup, you'd need to add the JAR to the Flink classpath
    print("⚠️  Note: Paimon connector JAR needs to be in Flink classpath")
    print(f"   JAR location: {os.path.abspath(paimon_jar)}")
    
    # Set up Paimon catalog
    warehouse_path = "file:///tmp/real_pyflink_paimon_test"
    print(f"📁 Warehouse path: {warehouse_path}")
    
    try:
        # Create Paimon catalog
        print("📋 Creating Paimon catalog...")
        table_env.execute_sql(f"""
            CREATE CATALOG paimon_catalog WITH (
                'type' = 'paimon',
                'warehouse' = '{warehouse_path}'
            )
        """)
        
        # Use the catalog
        table_env.execute_sql("USE CATALOG paimon_catalog")
        print("✅ Paimon catalog created and set as default")
        
        # Create the users table
        print("📋 Creating users table...")
        table_env.execute_sql("""
            CREATE TABLE users (
                id INT,
                name STRING,
                age INT,
                email STRING,
                active BOOLEAN,
                PRIMARY KEY (id) NOT ENFORCED
            )
        """)
        print("✅ Users table created")
        
        # Insert data using Table API
        print("📝 Inserting test data...")
        
        # Create data as a table
        data_table = table_env.from_elements([
            (1, 'Alice Johnson', 28, 'alice@example.com', True),
            (2, 'Bob Smith', 34, 'bob@example.com', True),
            (3, 'Charlie Brown', 25, 'charlie@example.com', False),
            (4, 'Diana Wilson', 42, 'diana@example.com', True),
            (5, 'Edward Davis', 31, 'edward@example.com', True)
        ], DataTypes.ROW([
            DataTypes.FIELD('id', DataTypes.INT()),
            DataTypes.FIELD('name', DataTypes.STRING()),
            DataTypes.FIELD('age', DataTypes.INT()),
            DataTypes.FIELD('email', DataTypes.STRING()),
            DataTypes.FIELD('active', DataTypes.BOOLEAN())
        ]))
        
        # Insert into Paimon table
        data_table.execute_insert("users").wait()
        print("✅ Test data inserted successfully")
        
        # Verify the data was inserted
        print("🔍 Verifying data...")
        result = table_env.execute_sql("SELECT COUNT(*) FROM users").collect()
        for row in result:
            count = row[0]
            print(f"✅ Table contains {count} rows")
        
        print("🎉 REAL Paimon table created successfully!")
        print(f"📍 Location: {warehouse_path}")
        print("\n🧪 Now test with DuckDB:")
        print(f"duckdb -c \"SELECT * FROM paimon_scan('{warehouse_path.replace('file://', '')}/users');\"")
        
        return True
        
    except Exception as e:
        print(f"❌ Error creating Paimon table: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = create_real_paimon_table()
    if success:
        print("\n🎯 SUCCESS: Real Paimon table created with PyFlink!")
        print("This table was created using the actual Paimon Flink connector.")
        print("Test our DuckDB extension against this real Paimon data.")
    else:
        print("\n❌ FAILED: Could not create real Paimon table")
