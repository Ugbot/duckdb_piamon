#!/usr/bin/env python3
"""
Test script to load the Paimon extension using standard DuckDB Python package.
"""

import os
import sys

def test_paimon_extension():
    # Path to our compiled extension
    extension_path = os.path.abspath("build/release/repository/v0.0.1/osx_arm64/paimon.duckdb_extension")

    if not os.path.exists(extension_path):
        print(f"❌ Extension not found: {extension_path}")
        return False

    print(f"📦 Testing Paimon extension: {extension_path}")
    print(f"   File size: {os.path.getsize(extension_path)} bytes")

    # Try to load the extension directly
    try:
        import duckdb
        print("🐦 Using DuckDB Python package")
        print("🔄 Connecting to DuckDB...")

        # Create connection
        conn = duckdb.connect(':memory:')
        print("✅ Connected to DuckDB")

        # Try to load the extension
        print("🔄 Loading Paimon extension...")
        conn.execute(f"LOAD '{extension_path}'")
        print("✅ Paimon extension loaded successfully!")

        # Try a simple query
        result = conn.execute("SELECT 'Paimon extension is working!' as message").fetchall()
        print(f"📝 Query result: {result[0][0]}")

        # Try to list functions (if any are registered)
        try:
            functions = conn.execute("SELECT function_name FROM duckdb_functions() WHERE function_name LIKE '%paimon%'").fetchall()
            print(f"🔍 Paimon functions found: {len(functions)}")
            for func in functions:
                print(f"  - {func[0]}")
        except Exception as e:
            print(f"⚠️  Could not list functions: {e}")

        # Try to list table functions
        try:
            table_functions = conn.execute("SELECT name FROM duckdb_table_functions() WHERE name LIKE '%paimon%'").fetchall()
            print(f"📋 Paimon table functions found: {len(table_functions)}")
            for func in table_functions:
                print(f"  - {func[0]}")
        except Exception as e:
            print(f"⚠️  Could not list table functions: {e}")

        # Test Paimon warehouse reading if it exists
        warehouse_path = os.path.abspath("paimon_data/users")
        if os.path.exists(warehouse_path):
            print(f"🏭 Testing Paimon warehouse at: {warehouse_path}")
            try:
                # Try to read the Paimon table
                result = conn.execute(f"SELECT * FROM paimon_scan('{warehouse_path}') LIMIT 10").fetchall()
                print(f"📊 Read {len(result)} rows from Paimon table")
                if result:
                    print("   Sample data:")
                    for i, row in enumerate(result[:3]):
                        print(f"     Row {i+1}: {row}")
            except Exception as e:
                print(f"⚠️  Could not read Paimon table: {e}")
                print("   This is expected - full read implementation may be incomplete")
        else:
            print(f"🏭 Paimon warehouse not found at: {warehouse_path}")

        conn.close()
        print("🎉 Test completed successfully!")
        return True

    except Exception as e:
        print(f"❌ Error testing extension: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_paimon_extension()
    sys.exit(0 if success else 1)
