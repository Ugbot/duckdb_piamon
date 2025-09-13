#!/usr/bin/env python3
"""
Script to create a Paimon warehouse with test data.
This creates the basic Paimon table structure and writes some test data.
"""

import os
import json
import uuid
from datetime import datetime
import random

def create_paimon_table(base_path, table_name, schema, data):
    """
    Create a basic Paimon table structure with test data.
    This is a simplified implementation for testing purposes.
    """

    # Create directory structure
    table_path = os.path.join(base_path, table_name)
    schema_path = os.path.join(table_path, "schema")
    snapshot_path = os.path.join(table_path, "snapshot")
    manifest_path = os.path.join(table_path, "manifest")
    data_path = os.path.join(table_path, "data")

    os.makedirs(schema_path, exist_ok=True)
    os.makedirs(snapshot_path, exist_ok=True)
    os.makedirs(manifest_path, exist_ok=True)
    os.makedirs(data_path, exist_ok=True)

    # Create schema file
    schema_file = os.path.join(schema_path, "schema-1")
    with open(schema_file, 'w') as f:
        json.dump({
            "id": 1,
            "type": "struct",
            "fields": schema
        }, f, indent=2)

    # Create manifest file
    manifest_file = os.path.join(manifest_path, "manifest-1")
    with open(manifest_file, 'w') as f:
        json.dump({
            "id": 1,
            "files": []
        }, f, indent=2)

    # Create snapshot file
    snapshot_file = os.path.join(snapshot_path, "snapshot-1")
    with open(snapshot_file, 'w') as f:
        json.dump({
            "id": 1,
            "schemaId": 1,
            "baseManifestList": "manifest/manifest-1",
            "deltaManifestList": None,
            "timestampMs": int(datetime.now().timestamp() * 1000),
            "summary": {
                "operation": "append",
                "paimon.table.uuid": str(uuid.uuid4())
            }
        }, f, indent=2)

    # Create some test data files (simplified - just JSON for now)
    for i, row in enumerate(data):
        data_file = os.path.join(data_path, f"data-{i+1}.json")
        with open(data_file, 'w') as f:
            json.dump(row, f, indent=2)

    print(f"✅ Created Paimon table '{table_name}' at {table_path}")
    print(f"   Schema: {len(schema)} fields")
    print(f"   Data rows: {len(data)}")
    print(f"   Files created: schema, snapshot, manifest, {len(data)} data files")

    return table_path

def generate_test_data():
    """Generate some test data for the Paimon table."""
    return [
        {"id": 1, "name": "Alice", "age": 25, "city": "New York"},
        {"id": 2, "name": "Bob", "age": 30, "city": "San Francisco"},
        {"id": 3, "name": "Charlie", "age": 35, "city": "Chicago"},
        {"id": 4, "name": "Diana", "age": 28, "city": "Boston"},
        {"id": 5, "name": "Eve", "age": 32, "city": "Seattle"}
    ]

def main():
    # Create paimon_data directory
    warehouse_path = "paimon_data"
    os.makedirs(warehouse_path, exist_ok=True)

    print(f"🏭 Creating Paimon warehouse at: {warehouse_path}")

    # Define schema
    schema = [
        {"id": 1, "name": "id", "type": "int", "nullable": False},
        {"id": 2, "name": "name", "type": "string", "nullable": False},
        {"id": 3, "name": "age", "type": "int", "nullable": False},
        {"id": 4, "name": "city", "type": "string", "nullable": False}
    ]

    # Generate test data
    test_data = generate_test_data()

    # Create the table
    table_name = "users"
    table_path = create_paimon_table(warehouse_path, table_name, schema, test_data)

    print("\n📊 Table Summary:")
    print(f"   Location: {table_path}")
    print(f"   Format: Paimon")
    print(f"   Schema: {len(schema)} columns")
    print(f"   Rows: {len(test_data)}")

    print("\n🎯 Paimon warehouse created successfully!")
    print("   You can now test reading this data with the Paimon extension")
    # Show the directory structure
    print("\n📁 Directory structure:")
    for root, dirs, files in os.walk(warehouse_path):
        level = root.replace(warehouse_path, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f"{indent}{os.path.basename(root)}/")
        subindent = ' ' * 2 * (level + 1)
        for file in files:
            print(f"{subindent}{file}")

if __name__ == "__main__":
    main()
