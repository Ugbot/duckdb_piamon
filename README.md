# DuckDB Paimon Extension

A native C++ DuckDB extension for reading, writing, and managing [Apache Paimon](https://paimon.apache.org/)
tables (Paimon 1.4 format). It is implemented directly against the Paimon on-disk spec — snapshots,
schemas, manifests (Avro), BinaryRow, deletion vectors, and the commit protocol — with **no JVM, no
Flink, and no Spark** required at runtime.

> **Status:** functional and cross-validated against the reference implementations (pypaimon 1.4.1 and
> Apache Spark + paimon-spark), but not yet hardened for production scale. Read the
> [capabilities and limitations](#capabilities--limitations) before relying on it — several Paimon
> features are deliberately out of scope, and a few are blocked by the surrounding DuckDB SQL surface.

This project began as a fork of the in-repo `duckdb-iceberg` extension and reuses its machinery
(multi-file reader, catalog plumbing, CRoaring, value decoding) where it fits.

---

## Build

Requires a vcpkg toolchain (for the Avro/zstd dependencies used by manifest and Avro-data-file reads).

```bash
# Paimon extension only:
cmake --build build/release --config Release --target paimon_loadable_extension

# Everything (iceberg + paimon):
make release VCPKG_BUILD=1
```

Output: `build/release/extension/iceberg/paimon.duckdb_extension`.

```sql
LOAD 'build/release/extension/iceberg/paimon.duckdb_extension';
```

The extension loads without the Avro extension; Avro is loaded lazily only if a table actually has
Avro data files (see [data file formats](#data-file-formats)).

---

## Quick start

```sql
-- Read a table by path
SELECT * FROM paimon_scan('/warehouse/db.db/my_table');

-- Or attach a warehouse as a catalog and use normal SQL
ATTACH '/warehouse/db.db' AS pm (TYPE paimon);
SELECT count(*) FROM pm.my_table;

CREATE TABLE pm.events (id BIGINT, ts TIMESTAMP, payload VARCHAR);
INSERT INTO pm.events VALUES (1, now(), 'hello');

-- Primary-key table: upsert / delete / update
CREATE TABLE pm.users (id BIGINT PRIMARY KEY, name VARCHAR);
INSERT INTO pm.users VALUES (1, 'a'), (2, 'b');
UPDATE pm.users SET name = 'z' WHERE id = 1;
DELETE FROM pm.users WHERE id = 2;
```

---

## Function reference

### Reads
| Function | Purpose |
|---|---|
| `paimon_scan(path [, named params])` | Read a table's active data (merge-on-read for PK tables). |
| `paimon_snapshots(path)` | List snapshots. |
| `paimon_metadata(path)` | Table metadata summary (schema id, fields, partition/primary keys, snapshot count). |
| `paimon_attach` / `ATTACH ... (TYPE paimon)` | Expose a warehouse directory as a DuckDB catalog. |

`paimon_scan` named parameters:

| Parameter | Meaning |
|---|---|
| `version => 'name'` | Time travel to a tag or branch (or a snapshot id as a string). |
| `snapshot_from_id => N` | Read as of snapshot id `N`. |
| `snapshot_from_timestamp => TS` | Read as of the snapshot live at timestamp `TS`. |
| `incremental_from => A, incremental_to => B` | Changelog/incremental read of the records in snapshots `(A, B]` (raw delta, no merge). |

### Writes (via an attached catalog)
`INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, `CREATE TABLE AS SELECT`, `DROP TABLE`. PK tables support
upsert/update/delete (including composite keys) through a rowid↔key bridge; appends carry prior
manifests forward and commit atomically.

### Management
| Function | Purpose |
|---|---|
| `paimon_compact(path)` | Rewrite active files into one and commit a COMPACT snapshot. |
| `paimon_create_tag(path, name [, snapshot_id])` | Pin a snapshot as a tag (defaults to latest). |
| `paimon_delete_tag(path, name)` | Remove a tag. |
| `paimon_tags(path)` | List tags as `(tag_name, snapshot_id)`. |
| `paimon_create_branch(path, name [, source_tag])` | Create a branch from a tag or the latest snapshot. |
| `paimon_delete_branch(path, name)` | Remove a branch. |
| `paimon_expire_snapshots(path, retain_max)` | Keep the newest `retain_max` snapshots; delete older ones and reclaim orphaned files. Tags pin their data against expiration. |

---

## Capabilities & limitations

This is the honest, tested picture of what the extension does and does not do, and **why**.

### ✅ Reads (complete and validated)
- **Append (unaware/fixed bucket) and partitioned tables** — manifest-driven file discovery, with a
  directory-scan fallback.
- **Primary-key merge-on-read**, honoring the table's `merge-engine`:
  - `deduplicate` (default) — highest `_SEQUENCE_NUMBER` wins, tombstones drop the key.
  - `partial-update` — latest non-null value per column (nulls do not overwrite).
  - `aggregation` — per-column `fields.<col>.aggregate-function` (sum/min/max/count/product/bool_and/
    bool_or/first_value/last_value; unconfigured columns default to last-non-null).
  - `first-row` — the first record per key wins.
  - `sequence.field` is honored as the merge ordering.
- **Deletion vectors** — DV-enabled PK tables resolve correctly via merge-on-read (validated against
  Spark at every snapshot).
- **Schema evolution** — add / drop / reorder / **rename** columns. Renames use Paimon's stable field
  ids, so a renamed column keeps its old data under the new name (matching Spark/Flink).
- **Time travel** — by snapshot id, timestamp, **tag**, or **branch**.
- **Incremental / changelog batch reads** — the raw delta of a snapshot range.
- **Predicate pushdown** — `WHERE` predicates are pushed into the underlying Parquet reader as file
  filters (row-group / file pruning), while DuckDB still re-applies the full filter for correctness.
- **Zstandard-compressed manifests** — read natively, so real Spark/Flink tables work (not just
  pypaimon's uncompressed output). Dynamic-bucket and file-index (bloom) tables read correctly.

### ✅ Writes & management (works)
- `INSERT` (append, atomic commit with carry-forward), PK **upsert / UPDATE / DELETE** (incl. composite
  keys), `CREATE TABLE`, `CREATE TABLE AS SELECT`, `DROP TABLE`.
- `paimon_compact`, tags, branches, snapshot expiration.
- All manifests are written by a **native Avro writer** — the Avro extension is not used at write time.

### ⚠️ Data file formats
- **Parquet** — fully supported (Paimon's default), read and written.
- **Avro data files** — read via DuckDB's `read_avro` (loaded lazily; mixed Parquet+Avro tables are
  unioned by name). Written tables always use Parquet.
- **ORC data files** — **cannot be read.** This DuckDB distribution ships no ORC reader (none exists in
  core or any bundled extension), so the scan raises a clear error rather than misreading. Rewrite the
  table with `file.format=parquet`, or read it with an ORC-capable engine.

### ❌ Not supported — and why
- **Partitioned / dynamic-bucket writes via SQL.** Blocked at the DuckDB layer: a custom catalog's
  `CREATE TABLE` has no way to express `PARTITIONED BY`, and dynamic bucketing needs the hash index.
  Partitioned tables written by other engines **read** fine; we just can't create them from SQL here.
- **Streaming / changelog *producers*** (`changelog-producer=input|lookup|full-compaction`). DuckDB is a
  batch query engine with no continuous-query or checkpoint model, so a streaming *write* source does
  not map onto it — there is nothing to drive incremental commits. **Batch and incremental *reads* of
  changelog data already work** (`incremental_from`/`incremental_to`); it is only the streaming
  write-side that is out of scope by design.
- **ORC writing/reading** — see above (no ORC codec available).
- **Manifest-level statistics skipping.** Files are already pruned by Parquet row-group statistics via
  predicate pushdown; decoding `_VALUE_STATS` to skip a file *before* opening its footer is a marginal
  optimization that is not implemented.
- **Partial-update delete-retraction and aggregation sequence-groups.** The common merge cases are
  correct; delete records are excluded from those two engines rather than fully modelled.

---

## Validation

Fixtures are produced by the **reference implementations** and read back by this extension:

- `make paimon_data` — generates tables with **pypaimon 1.4.1** (the reference Python implementation).
- `make paimon_spark_data` — generates deletion-vector / dynamic-bucket / file-index / zstandard-manifest
  tables with **Apache Spark + paimon-spark** (needs a container + Maven access).

Tests live in `test/sql/local/paimon_*.test` (sqllogictest) and run with:

```bash
DUCKDB_PAIMON_HAVE_GENERATED_DATA=1 build/release/test/unittest "test/sql/local/paimon_*.test"
# Spark-derived fixtures additionally need DUCKDB_PAIMON_HAVE_SPARK_DATA=1
```

### Running with assertions

The binary parsers and the commit path are written defensively: untrusted input (manifest/data bytes,
hint files) is validated with runtime `throw`s that are always on, while internal invariants use
`D_ASSERT`, which compiles out of a Release build. To exercise those assertions, build with them forced
on and run the same suites against that binary:

```bash
make relassert VCPKG_TOOLCHAIN_PATH=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
# then load build/relassert/.../paimon.duckdb_extension into build/relassert/test/unittest
```

A truncated/garbage manifest must never crash or read out of bounds — the `corrupt_manifest` fixture
asserts the reader degrades gracefully (falls back to directory discovery) and still returns all rows.

> **A note on the oracle:** pypaimon's *Python reader* does not implement several features it can
> *write* — column rename mapping, the non-default merge engines, and branch creation/listing all
> return wrong or unsupported results there. For those, pypaimon is used as the authoritative *writer*
> and reads are asserted against the **Paimon/Spark spec result** (computed in the fixture, and
> cross-checked against Spark where available). In these cases this extension is *more* correct than
> pypaimon's own reader.

---

## Project layout

```
src/
  paimon_extension.cpp        Entry point; registers functions
  paimon_functions.cpp        Table functions: scan, snapshots, metadata, attach, compact,
                              tags/branches, expire; merge engines; predicate pushdown; format dispatch
  paimon_metadata.cpp         Schema/snapshot parsing, type system, tag/branch path resolution
  paimon_manifest.cpp         Manifest-list / manifest reading (native Avro), active-file computation
  paimon_avro_reader.cpp      Native Avro OCF decoder (null + zstandard)
  paimon_avro_writer.cpp      Native Avro OCF writer (manifests)
  paimon_binary_row.cpp       BinaryRow decoder (_PARTITION, stats, keys)
  paimon_multi_file_*.cpp     File listing + multi-file reader integration
  paimon_predicate.cpp        Predicate translation helpers
  storage/                    Catalog integration: ATTACH, INSERT/DELETE/UPDATE, schema/table entries
scripts/data_generators/      pypaimon + Spark fixture generators
test/sql/local/               sqllogictest suites (append, primary_key, write, deletion_vectors)
paimon/                        Apache Paimon reference (git submodule)
```

See `CLAUDE.md` for build/contribution conventions.

## License

See [LICENSE](LICENSE). Apache Paimon is a trademark of the Apache Software Foundation.
