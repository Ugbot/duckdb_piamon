#!/usr/bin/env python3
"""
Test script to load the Paimon extension using our custom DuckDB build.
"""

import os
import sys
import subprocess

def test_paimon_extension():
    # Path to our compiled extension
    extension_path = os.path.abspath("build/release/repository/v0.0.1/osx_arm64/paimon.duckdb_extension")

    if not os.path.exists(extension_path):
        print(f"❌ Extension not found: {extension_path}")
        return False

    # Path to our custom DuckDB binary (version-matched)
    duckdb_binary = os.path.abspath("build/release/duckdb")

    if not os.path.exists(duckdb_binary):
        print(f"❌ DuckDB binary not found: {duckdb_binary}")
        return False

    print(f"📦 Testing Paimon extension: {extension_path}")
    print(f"   File size: {os.path.getsize(extension_path)} bytes")
    print(f"🐦 Using custom DuckDB binary: {duckdb_binary}")

    # Create test database file first (to avoid autoloading issues)
    test_db = "/tmp/paimon_test.db"

    # Clean up any existing test database
    if os.path.exists(test_db):
        os.remove(test_db)

    try:
        print("🔄 Creating test database and testing Paimon extension...")

        # Combine all SQL into one execution
        full_test_sql = f"""
-- Disable autoinstall to prevent issues
SET autoinstall_known_extensions = false;

-- Load parquet first
LOAD parquet;

-- Load our Paimon extension
LOAD '{extension_path}';

-- Check all paimon functions
SELECT '=== PAIMON FUNCTIONS ===' as header;
SELECT function_name
FROM duckdb_functions()
WHERE function_name LIKE '%paimon%'
ORDER BY function_name;

-- Test basic functionality
SELECT '=== EXTENSION TEST ===' as header;
SELECT 'Paimon extension loaded successfully with ' || COUNT(*) || ' functions!' as status
FROM duckdb_functions()
WHERE function_name LIKE '%paimon%';
"""

        result = subprocess.run(
            [duckdb_binary, "-unsigned", test_db],
            input=full_test_sql,
            capture_output=True,
            text=True,
            timeout=30
        )

        print("📝 Test results:")
        print(result.stdout)

        if result.stderr:
            print("⚠️  Test stderr:")
            print(result.stderr)

        if result.returncode == 0:
            print("🎉 Paimon extension test completed successfully!")
            return True
        else:
            print(f"❌ Test failed with return code: {result.returncode}")
            return False

    except subprocess.TimeoutExpired:
        print("❌ Test timed out")
        return False
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Clean up test database
        if os.path.exists(test_db):
            os.remove(test_db)

if __name__ == "__main__":
    success = test_paimon_extension()
    sys.exit(0 if success else 1)
