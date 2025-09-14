#!/usr/bin/env python3
"""
Create a test Paimon table structure manually to test compatibility
"""

import os
import json
import uuid
from datetime import datetime

def create_test_paimon_table():
    """Create a minimal Paimon table structure for testing"""
    
    # Create directory structure
    base_path = "test_real_paimon/test_table"
    bucket_path = f"{base_path}/bucket-0"
    manifest_path = f"{base_path}/manifest"
    snapshot_path = f"{base_path}/snapshot"
    
    os.makedirs(bucket_path, exist_ok=True)
    os.makedirs(manifest_path, exist_ok=True)
    os.makedirs(snapshot_path, exist_ok=True)
    
    print("📁 Created directory structure:")
    print(f"  {bucket_path}/")
    print(f"  {manifest_path}/")
    print(f"  {snapshot_path}/")
    
    # Generate UUIDs
    data_uuid = str(uuid.uuid4())
    manifest_uuid = str(uuid.uuid4())
    
    # Create a simple ORC data file (we'll create a placeholder)
    data_file = f"{bucket_path}/data-{data_uuid}-0.orc"
    with open(data_file, 'w') as f:
        f.write("# Placeholder ORC file content\n")
        f.write("# In reality this would be binary ORC format\n")
    print(f"📄 Created data file: {data_file}")
    
    # Create manifest file (Avro format - we'll create JSON for now as placeholder)
    manifest_file = f"{manifest_path}/manifest-{manifest_uuid}-0.avro"
    
    # Create a manifest entry that matches our implementation
    manifest_content = {
        "entries": [{
            "_KIND": 0,  # ADD
            "_PARTITION": [],
            "_BUCKET": 0,
            "_TOTAL_BUCKETS": 1,
            "_FILE": {
                "_FILE_NAME": f"bucket-0/data-{data_uuid}-0.orc",
                "_FILE_SIZE": 1024,
                "_ROW_COUNT": 3,
                "_MIN_KEY": [],
                "_MAX_KEY": [],
                "_KEY_STATS": {
                    "colNames": ["id"],
                    "colStats": [{"min": 1, "max": 3, "nullCount": 0}]
                },
                "_VALUE_STATS": {
                    "colNames": ["id", "name", "age"],
                    "colStats": [
                        {"min": 1, "max": 3, "nullCount": 0},
                        {"min": None, "max": None, "nullCount": 0},
                        {"min": 25, "max": 35, "nullCount": 0}
                    ]
                },
                "_MIN_SEQUENCE_NUMBER": 1,
                "_MAX_SEQUENCE_NUMBER": 3,
                "_SCHEMA_ID": 0,
                "_LEVEL": 0,
                "_EXTRA_FILES": [],
                "_CREATION_TIME": int(datetime.now().timestamp() * 1000),
                "_DELETE_ROW_COUNT": None,
                "_EMBEDDED_FILE_INDEX": None,
                "_FILE_SOURCE": 0,  # APPEND
                "_VALUE_STATS_COLS": ["id", "name", "age"],
                "_EXTERNAL_PATH": None,
                "_FIRST_ROW_ID": None,
                "_WRITE_COLS": ["id", "name", "age"]
            }
        }]
    }
    
    # For now, save as JSON (in reality this should be Avro)
    # We'll test if our extension can read JSON manifests
    with open(manifest_file.replace('.avro', '.json'), 'w') as f:
        json.dump(manifest_content, f, indent=2)
    print(f"📋 Created manifest file: {manifest_file.replace('.avro', '.json')}")
    
    # Create manifest list file
    manifest_list_uuid = str(uuid.uuid4())
    manifest_list_file = f"{manifest_path}/manifest-list-{manifest_list_uuid}-0.avro"
    
    manifest_list_content = {
        "entries": [{
            "_FILE_NAME": f"manifest-{manifest_uuid}-0.avro",
            "_FILE_SIZE": 2048,
            "_NUM_ADDED_FILES": 1,
            "_NUM_DELETED_FILES": 0,
            "_PARTITION_STATS": {
                "colNames": [],
                "colStats": [],
                "nullCount": 0
            },
            "_SCHEMA_ID": 0,
            "_MIN_BUCKET": 0,
            "_MAX_BUCKET": 0,
            "_MIN_LEVEL": 0,
            "_MAX_LEVEL": 0
        }]
    }
    
    with open(manifest_list_file.replace('.avro', '.json'), 'w') as f:
        json.dump(manifest_list_content, f, indent=2)
    print(f"📋 Created manifest list file: {manifest_list_file.replace('.avro', '.json')}")
    
    # Create snapshot file
    snapshot_content = {
        "version": 3,
        "id": 1,
        "schemaId": 0,
        "baseManifestList": "",
        "deltaManifestList": f"manifest/manifest-list-{manifest_list_uuid}-0.avro",
        "deltaManifestListSize": 1024,
        "changelogManifestList": None,
        "indexManifest": None,
        "commitUser": "test-user",
        "commitIdentifier": 9223372036854775807,
        "commitKind": "APPEND",
        "timeMillis": int(datetime.now().timestamp() * 1000),
        "logOffsets": {},
        "totalRecordCount": 3,
        "deltaRecordCount": 3,
        "watermark": -9223372036854775808
    }
    
    snapshot_file = f"{snapshot_path}/snapshot-1"
    with open(snapshot_file, 'w') as f:
        json.dump(snapshot_content, f, indent=2)
    print(f"📋 Created snapshot file: {snapshot_file}")
    
    # Create EARLIEST and LATEST pointers
    with open(f"{snapshot_path}/EARLIEST", 'w') as f:
        f.write("snapshot-1")
    with open(f"{snapshot_path}/LATEST", 'w') as f:
        f.write("snapshot-1")
    print("📋 Created EARLIEST and LATEST pointers")
    
    print("\n✅ Test Paimon table created successfully!")
    print(f"📍 Location: {base_path}")
    print("\n🧪 Ready for DuckDB Paimon extension testing")

if __name__ == "__main__":
    create_test_paimon_table()
