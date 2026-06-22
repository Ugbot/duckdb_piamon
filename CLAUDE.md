# DuckDB Paimon Extension

## LLM Station Tools

You have access to LLM Station's code intelligence via shell commands. The daemon is running and has indexed this workspace. **Use these tools for search and analysis instead of manual file reading when exploring code structure.**

### Search code by pattern
```bash
/Users/bengamble/llm-station/build/llm-station-mcp run grep_search pattern="PATTERN" path=src/
```

### List all available tools
```bash
/Users/bengamble/llm-station/build/llm-station-mcp list-tools
```

### Search for design patterns and templates
```bash
/Users/bengamble/llm-station/build/llm-station-mcp run crystal_search query="multi file reader"
```

## Build

```bash
# Build paimon extension only:
cmake --build build/release --config Release --target paimon_loadable_extension

# Build everything (iceberg + paimon):
cmake --build build/release --config Release
```

The paimon extension is a separate target defined in CMakeLists.txt lines 86-107.

### Full build with Avro support (required for manifest reading)

Both iceberg and paimon need the avro extension for reading manifest files.
The avro extension requires VCPKG with duckdb's custom avro-c fork:

```bash
# 1. Uncomment avro in extension_config.cmake
# 2. Build with VCPKG — pass the toolchain path explicitly. VCPKG_BUILD=1 alone does NOT wire in the
#    toolchain (the avro extension then fails to find its static deps, e.g. libz.a):
make release VCPKG_TOOLCHAIN_PATH=/Users/bengamble/vcpkg/scripts/buildsystems/vcpkg.cmake
```

Without avro, paimon falls back to directory-based file discovery (scans bucket dirs).

### Assertion build (exercise D_ASSERT)

`D_ASSERT` compiles out of Release. To run the suites with internal invariants active, build the
`relassert` target (RelWithDebInfo + FORCE_ASSERT=1) with the same toolchain path, then load that
extension into `build/relassert/test/unittest`:

```bash
make relassert VCPKG_TOOLCHAIN_PATH=/Users/bengamble/vcpkg/scripts/buildsystems/vcpkg.cmake
```

Defensive convention: untrusted external input (manifest/data bytes, hint files) is validated with
runtime `throw`s that are always on; only internal "can't happen" invariants use `D_ASSERT`.

### Extension output
- `build/release/extension/iceberg/paimon.duckdb_extension`
- `build/release/extension/iceberg/iceberg.duckdb_extension`

## Approach

- Fix ONE file at a time
- After each fix, try to build
- If build errors decrease, commit the change
- If build errors increase or stay the same, reconsider the approach
- Do NOT modify anything in the `duckdb/` submodule
- Focus on making it compile first, correct behavior second
- Use `git diff` to review your changes before committing

## Project Structure

This is a DuckDB extension for reading Apache Paimon table format data.
It was forked from the duckdb-iceberg extension and is being converted.

Key source files:
- `src/paimon_extension.cpp` — Extension entry point, registers functions
- `src/paimon_functions.cpp` — Table functions (paimon_scan, paimon_snapshots, paimon_metadata, paimon_attach)
- `src/paimon_metadata.cpp` — Paimon table metadata parsing (schema, snapshots, type system)
- `src/paimon_manifest.cpp` — Manifest-list and manifest file reading (Avro format via read_avro)
- `src/paimon_multi_file_reader.cpp` — Multi-file reader for parquet data files
- `src/paimon_multi_file_list.cpp` — File listing (manifest-driven with directory fallback)
- `src/paimon_predicate.cpp` — Predicate pushdown for scan filtering
- `src/storage/paimon_catalog.cpp` — DuckDB catalog integration (ATTACH, PlanInsert)
- `src/storage/paimon_insert.cpp` — INSERT write path (parquet writing + manifest/snapshot commit)
- `src/storage/paimon_schema_entry.cpp` — Schema entry for catalog
- `src/storage/paimon_table_entry.cpp` — Table entry for catalog

Headers in `src/include/` and `src/include/storage/`.

Shared utility from iceberg: `src/common/utils.cpp` (IcebergUtils::FileToString, etc.)

## Reference Implementation

alibaba/paimon-cpp is the authoritative C++ Paimon implementation (~5000 files).
Use it as reference for Paimon format details (manifest schema, commit protocol, etc.).
