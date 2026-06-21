#!/usr/bin/env bash
# Generate Paimon fixtures that pypaimon's batch path cannot (deletion vectors), using the
# reference Spark+Paimon engine in a container. Produces a deletion-vectors-enabled primary-key
# table under <warehouse>/paimon_spark.db/ for cross-engine validation of the DuckDB reader.
set -euo pipefail

WAREHOUSE="${1:-$(pwd)/data/generated/paimon_spark}"
SPARK_IMAGE="${SPARK_IMAGE:-apache/spark:3.5.3}"
PAIMON_PKG="${PAIMON_PKG:-org.apache.paimon:paimon-spark-3.5:1.0.1}"

rm -rf "$WAREHOUSE"
mkdir -p "$WAREHOUSE"
chmod 777 "$WAREHOUSE"  # the Spark container runs as a non-root uid; let it write the warehouse

read -r -d '' SQL <<'EOSQL' || true
CREATE TABLE paimon.default.pk_dv (id INT, v STRING)
  TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'deletion-vectors.enabled'='true',
                 'manifest.compression'='null');
INSERT INTO paimon.default.pk_dv VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
UPDATE paimon.default.pk_dv SET v='B2' WHERE id=2;
DELETE FROM paimon.default.pk_dv WHERE id=4;
INSERT INTO paimon.default.pk_dv VALUES (6,'f');
EOSQL

podman run --rm \
  -v "$WAREHOUSE":/work/warehouse:Z \
  --env HOME=/tmp \
  "$SPARK_IMAGE" \
  /opt/spark/bin/spark-sql \
    --packages "$PAIMON_PKG" \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
    --conf spark.sql.catalog.paimon.warehouse=/work/warehouse \
    --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
    -e "$SQL"

echo "Spark/Paimon fixtures written to: $WAREHOUSE"
find "$WAREHOUSE" -type d -name index 2>/dev/null && echo "(deletion-vector index files present)"
