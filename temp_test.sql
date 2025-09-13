SET autoinstall_known_extensions = false;
LOAD 'build/release/repository/v0.0.1/osx_arm64/parquet.duckdb_extension';
LOAD 'build/release/repository/v0.0.1/osx_arm64/httpfs.duckdb_extension';
SELECT 'Extensions loaded' as status;
