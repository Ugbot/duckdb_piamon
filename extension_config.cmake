# This file is included by DuckDB's build system. It specifies which extension to load
# Avro disabled - our Paimon extension doesn't need it
# duckdb_extension_load(avro
# 		LOAD_TESTS
# 		GIT_URL https://github.com/duckdb/duckdb-avro
# 		GIT_TAG 0c97a61781f63f8c5444cf3e0c6881ecbaa9fe13
# )

# Extension from this repo - temporarily disabled to test Paimon only
# duckdb_extension_load(iceberg
#     SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
#     LOAD_TESTS
#     LINKED_LIBS "../../vcpkg_installed/wasm32-emscripten/lib/*.a"
# )

duckdb_extension_load(tpch)
duckdb_extension_load(icu)
duckdb_extension_load(ducklake
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/ducklake
        GIT_TAG c1ebd032eb4c763910551c08f4b61bdb8168f209
)

# Add ORC and JSON support for Paimon multi-format tables
# Note: These may not be available in all DuckDB builds, so Paimon will gracefully fall back
# duckdb_extension_load(json)
# duckdb_extension_load(orc)


if (NOT EMSCRIPTEN)
################## AWS
if (NOT MINGW)
    duckdb_extension_load(aws
            LOAD_TESTS
            GIT_URL https://github.com/duckdb/duckdb-aws
            GIT_TAG f855eb3dce37700bfd36fe906a683e4be17dcaf6
    )
endif ()
endif()


