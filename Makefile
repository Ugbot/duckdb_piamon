PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=iceberg
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# We need this for testing
CORE_EXTENSIONS='httpfs;parquet;tpch'

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

start-rest-catalog: install_requirements
	./scripts/start-rest-catalog.sh

install_requirements:
	python3 -m pip install -r scripts/requirements.txt

# Custom makefile targets
data: data_clean start-rest-catalog
	python3 -m scripts.data_generators.generate_data spark-rest local

data_large: data data_clean
	python3 -m scripts.data_generators.generate_data spark-rest local

data_clean:
	rm -rf data/generated

# Generate native Apache Paimon fixtures via pypaimon (used by test/sql/local/paimon_*.test).
# Run tests with DUCKDB_PAIMON_HAVE_GENERATED_DATA=1 after this.
paimon-fixtures-venv:
	python3.12 -m venv .paimon-fixtures-venv
	.paimon-fixtures-venv/bin/pip install --quiet --upgrade pip
	.paimon-fixtures-venv/bin/pip install --quiet pypaimon==1.4.1 requests

paimon_data: paimon-fixtures-venv
	.paimon-fixtures-venv/bin/python scripts/data_generators/generate_paimon_fixtures.py

# Deletion-vector / advanced fixtures from the reference Spark+Paimon engine (needs a container +
# Maven access). Run tests with DUCKDB_PAIMON_HAVE_SPARK_DATA=1 afterwards.
paimon_spark_data:
	bash scripts/data_generators/generate_paimon_spark_fixtures.sh

wasm_pre_build_step:
	jq 'del(.overrides,.dependencies[5])' vcpkg.json | unexpand -t2 > vcpkg.json.tmp && mv vcpkg.json.tmp vcpkg.json
