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


def gen_pk_partial_update(catalog, db):
    """Primary-key table with merge-engine=partial-update. Later non-null values overwrite per column;
    nulls do NOT overwrite. (pypaimon's own reader does not implement this merge, so the expected
    values below are the Paimon/Spark spec result, computed by hand.)"""
    schema = Schema.from_pyarrow_schema(
        pa.schema([("id", pa.int64()), ("a", pa.int64()), ("b", pa.int64()), ("c", pa.string())]),
        primary_keys=["id"],
        options={"bucket": "1", "merge-engine": "partial-update"},
    )
    catalog.create_table(f"{db}.pk_partial", schema, False)
    table = catalog.get_table(f"{db}.pk_partial")
    # Commit 1: full rows for keys 1..3.
    write(table, pa.record_batch({
        "id": pa.array([1, 2, 3], pa.int64()),
        "a": pa.array([10, 20, 30], pa.int64()),
        "b": pa.array([None, 200, None], pa.int64()),
        "c": pa.array(["x1", None, "x3"], pa.string()),
    }))
    # Commit 2: partial updates — only some columns set per key (nulls must not clobber).
    write(table, pa.record_batch({
        "id": pa.array([1, 2, 3], pa.int64()),
        "a": pa.array([None, None, 99], pa.int64()),   # id1 keeps 10, id2 keeps 20, id3 -> 99
        "b": pa.array([111, None, 333], pa.int64()),   # id1 -> 111, id2 keeps 200, id3 -> 333
        "c": pa.array([None, "y2", None], pa.string()), # id1 keeps x1, id2 -> y2, id3 keeps x3
    }))
    # Expected (spec): 1 -> (10,111,x1), 2 -> (20,200,y2), 3 -> (99,333,x3)
    return ("pk_partial", 3)


def gen_pk_aggregation(catalog, db):
    """Primary-key table with merge-engine=aggregation: each value column has its own agg function
    applied across all records of a key (sum/max/min). pypaimon's reader returns the raw unmerged
    rows, so the expected aggregated values below are the Paimon/Spark spec result."""
    schema = Schema.from_pyarrow_schema(
        pa.schema([("id", pa.int64()), ("total", pa.int64()), ("hi", pa.int64()), ("lo", pa.int64())]),
        primary_keys=["id"],
        options={
            "bucket": "1",
            "merge-engine": "aggregation",
            "fields.total.aggregate-function": "sum",
            "fields.hi.aggregate-function": "max",
            "fields.lo.aggregate-function": "min",
        },
    )
    catalog.create_table(f"{db}.pk_agg", schema, False)
    table = catalog.get_table(f"{db}.pk_agg")
    rows = [  # (id, total, hi, lo) across three commits
        ([1, 2], [10, 5], [10, 5], [10, 5]),
        ([1, 2], [3, 7], [99, 1], [1, 99]),
        ([1], [2], [50], [50]),
    ]
    for ids, tot, hi, lo in rows:
        write(table, pa.record_batch({
            "id": pa.array(ids, pa.int64()),
            "total": pa.array(tot, pa.int64()),
            "hi": pa.array(hi, pa.int64()),
            "lo": pa.array(lo, pa.int64()),
        }))
    # Expected: id1 total=15 hi=99 lo=1 ; id2 total=12 hi=5 lo=5
    return ("pk_agg", 2)


def gen_pk_first_row(catalog, db):
    """Primary-key table with merge-engine=first-row: the FIRST record per key wins; later records for
    the same key are ignored. pypaimon's reader returns last-write-wins, so the expected first-write
    values below are the Paimon/Spark spec result."""
    schema = Schema.from_pyarrow_schema(
        pa.schema([("id", pa.int64()), ("v", pa.int64())]),
        primary_keys=["id"],
        options={"bucket": "1", "merge-engine": "first-row"},
    )
    catalog.create_table(f"{db}.pk_firstrow", schema, False)
    table = catalog.get_table(f"{db}.pk_firstrow")
    write(table, pa.record_batch({"id": pa.array([1, 2, 3], pa.int64()), "v": pa.array([100, 200, 300], pa.int64())}))
    write(table, pa.record_batch({"id": pa.array([1, 2], pa.int64()), "v": pa.array([999, 888], pa.int64())}))
    # Expected: id1=100, id2=200, id3=300 (first writes win)
    return ("pk_firstrow", 3)


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
    for gen in (gen_append, gen_partitioned, gen_pk, gen_pk_multi, gen_evolve, gen_tagged, gen_rename,
                gen_pk_partial_update, gen_pk_aggregation, gen_pk_first_row):
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
