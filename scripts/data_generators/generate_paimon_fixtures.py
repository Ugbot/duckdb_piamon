#!/usr/bin/env python3
"""Generate native Apache Paimon (1.4.x) test fixtures using pypaimon.

These are authoritative cross-engine fixtures: tables are written by the real Paimon
Python implementation, then read back by the DuckDB paimon extension to validate parity.

Usage:
    .paimon-fixtures-venv/bin/python scripts/data_generators/generate_paimon_fixtures.py [warehouse_dir]

Covers: append (unaware-bucket), partitioned append, primary-key (deduplicate),
and primary-key with deletion-vectors enabled (UPDATE then read).
Data is randomized (not hardcoded happy-path rows).
"""
import os
import shutil
import sys
import random
import string

import pyarrow as pa
from pypaimon import CatalogFactory, Schema

# Deterministic fixtures: randomized *values* (per the project's no-hardcoded-data rule) but a
# fixed seed and fixed row counts so sqllogictest assertions are reproducible.
random.seed(42)


def rand_str(n=8):
    return "".join(random.choice(string.ascii_lowercase) for _ in range(n))


def make_catalog(warehouse):
    return CatalogFactory.create({"warehouse": warehouse})


def write(table, record_batch):
    wb = table.new_batch_write_builder()
    tw = wb.new_write()
    tc = wb.new_commit()
    tw.write_arrow_batch(record_batch)
    commit_messages = tw.prepare_commit()
    tc.commit(commit_messages)
    tw.close()


def gen_append(catalog, db):
    """Append-only unaware-bucket table with randomized rows."""
    schema = Schema.from_pyarrow_schema(
        pa.schema([
            ("id", pa.int64()),
            ("name", pa.string()),
            ("score", pa.float64()),
        ]),
        options={"bucket": "-1"},
    )
    catalog.create_table(f"{db}.append_simple", schema, False)
    table = catalog.get_table(f"{db}.append_simple")
    n = 20
    batch = pa.record_batch({
        "id": pa.array(list(range(n)), pa.int64()),
        "name": pa.array([rand_str() for _ in range(n)], pa.string()),
        "score": pa.array([round(random.uniform(0, 100), 3) for _ in range(n)], pa.float64()),
    })
    write(table, batch)
    return ("append_simple", n)


def gen_partitioned(catalog, db):
    """Partitioned append table (partition key dt)."""
    schema = Schema.from_pyarrow_schema(
        pa.schema([
            ("id", pa.int64()),
            ("dt", pa.string()),
            ("val", pa.int64()),
        ]),
        partition_keys=["dt"],
        options={"bucket": "-1"},
    )
    catalog.create_table(f"{db}.append_partitioned", schema, False)
    table = catalog.get_table(f"{db}.append_partitioned")
    parts = ["2024-01-01", "2024-01-02", "2024-01-03"]
    n = 50
    ids = list(range(n))
    dts = [parts[i % len(parts)] for i in range(n)]  # deterministic partition assignment
    vals = [random.randint(0, 1000) for _ in range(n)]
    batch = pa.record_batch({
        "id": pa.array(ids, pa.int64()),
        "dt": pa.array(dts, pa.string()),
        "val": pa.array(vals, pa.int64()),
    })
    write(table, batch)
    return ("append_partitioned", n)


def gen_pk(catalog, db):
    """Primary-key table (deduplicate merge engine) with an overwriting second commit."""
    schema = Schema.from_pyarrow_schema(
        pa.schema([
            ("id", pa.int64()),
            ("payload", pa.string()),
            ("version", pa.int64()),
        ]),
        primary_keys=["id"],
        options={"bucket": "1"},
    )
    catalog.create_table(f"{db}.pk_dedup", schema, False)
    table = catalog.get_table(f"{db}.pk_dedup")
    keys = list(range(50))
    batch1 = pa.record_batch({
        "id": pa.array(keys, pa.int64()),
        "payload": pa.array([rand_str() for _ in keys], pa.string()),
        "version": pa.array([1] * len(keys), pa.int64()),
    })
    write(table, batch1)
    # Second commit updates a subset of keys -> latest value must win on read.
    upd = random.sample(keys, 20)
    batch2 = pa.record_batch({
        "id": pa.array(upd, pa.int64()),
        "payload": pa.array([rand_str() for _ in upd], pa.string()),
        "version": pa.array([2] * len(upd), pa.int64()),
    })
    write(table, batch2)
    return ("pk_dedup", len(keys))


def gen_pk_multi(catalog, db):
    """Primary-key table with three commits to exercise multi-snapshot merge-on-read.

    NOTE on deletion vectors: pypaimon batch writes do not run compaction, so a
    `deletion-vectors.enabled` table cannot be produced in a readable state this way
    (pypaimon itself reads it back as 0 rows). Deletion-vector raw read is therefore
    validated separately against a compaction-bearing fixture (Flink/Spark)."""
    schema = Schema.from_pyarrow_schema(
        pa.schema([
            ("id", pa.int64()),
            ("payload", pa.string()),
            ("version", pa.int64()),
        ]),
        primary_keys=["id"],
        options={"bucket": "1"},
    )
    catalog.create_table(f"{db}.pk_multi", schema, False)
    table = catalog.get_table(f"{db}.pk_multi")
    keys = list(range(40))
    for v in (1, 2, 3):
        subset = keys if v == 1 else sorted(random.sample(keys, 15))
        write(table, pa.record_batch({
            "id": pa.array(subset, pa.int64()),
            "payload": pa.array([rand_str() for _ in subset], pa.string()),
            "version": pa.array([v] * len(subset), pa.int64()),
        }))
    return ("pk_multi", len(keys))


def gen_evolve(catalog, db):
    """Append table whose schema evolves (add a column) between commits, so data files exist under
    multiple schema ids. Readers must surface the added column as NULL for older files."""
    from pypaimon.schema.schema_change import SchemaChange
    from pypaimon.schema.data_types import DataTypeParser

    schema = Schema.from_pyarrow_schema(
        pa.schema([("id", pa.int64()), ("a", pa.int64())]),
        options={"bucket": "-1"},
    )
    catalog.create_table(f"{db}.evolve", schema, False)
    table = catalog.get_table(f"{db}.evolve")
    write(table, pa.record_batch({
        "id": pa.array([0, 1, 2], pa.int64()),
        "a": pa.array([10, 11, 12], pa.int64()),
    }))
    # Add column b, then write rows that populate it.
    catalog.alter_table(f"{db}.evolve", [SchemaChange.add_column("b", DataTypeParser.parse_data_type("STRING"))], False)
    table = catalog.get_table(f"{db}.evolve")
    write(table, pa.record_batch({
        "id": pa.array([3, 4], pa.int64()),
        "a": pa.array([13, 14], pa.int64()),
        "b": pa.array(["x", "y"], pa.string()),
    }))
    return ("evolve", 5)


def gen_rename(catalog, db):
    """Append table with a column RENAME between commits. Field ids are stable, so the renamed
    column's old data must be preserved under the new name (matches Spark/Flink; pypaimon's own
    reader returns NULL here, so this is validated against Spark, not pypaimon)."""
    from pypaimon.schema.schema_change import SchemaChange

    schema = Schema.from_pyarrow_schema(
        pa.schema([("id", pa.int64()), ("a", pa.int64())]),
        options={"bucket": "-1"},
    )
    catalog.create_table(f"{db}.renamed", schema, False)
    table = catalog.get_table(f"{db}.renamed")
    write(table, pa.record_batch({"id": pa.array([1, 2], pa.int64()), "a": pa.array([10, 20], pa.int64())}))
    catalog.alter_table(f"{db}.renamed", [SchemaChange.rename_column("a", "b")], False)
    table = catalog.get_table(f"{db}.renamed")
    write(table, pa.record_batch({"id": pa.array([3], pa.int64()), "b": pa.array([30], pa.int64())}))
    return ("renamed", 3)


def gen_tagged(catalog, db):
    """Append table with a tag created at the first snapshot, for tag time-travel tests."""
    schema = Schema.from_pyarrow_schema(
        pa.schema([("id", pa.int64()), ("v", pa.int64())]),
        options={"bucket": "-1"},
    )
    catalog.create_table(f"{db}.tagged", schema, False)
    table = catalog.get_table(f"{db}.tagged")
    write(table, pa.record_batch({"id": pa.array([1, 2], pa.int64()), "v": pa.array([10, 20], pa.int64())}))
    table.create_tag("v1")  # tag at snapshot 1 (2 rows)
    table = catalog.get_table(f"{db}.tagged")
    write(table, pa.record_batch({"id": pa.array([3], pa.int64()), "v": pa.array([30], pa.int64())}))
    return ("tagged", 3)


def main():
    warehouse = sys.argv[1] if len(sys.argv) > 1 else os.path.abspath("data/generated/paimon")
    if os.path.exists(warehouse):
        shutil.rmtree(warehouse)
    os.makedirs(warehouse, exist_ok=True)

    catalog = make_catalog(warehouse)
    db = "default"
    try:
        catalog.create_database(db, False)
    except Exception:
        pass

    results = []
    for gen in (gen_append, gen_partitioned, gen_pk, gen_pk_multi, gen_evolve, gen_tagged, gen_rename):
        try:
            results.append(gen(catalog, db))
            print(f"  [ok] {results[-1][0]}: {results[-1][1]} distinct keys/rows")
        except Exception as e:
            print(f"  [FAIL] {gen.__name__}: {e}")
            raise

    print(f"\nPaimon fixtures written to: {warehouse}")
    print("Tables:", ", ".join(r[0] for r in results))


if __name__ == "__main__":
    main()
