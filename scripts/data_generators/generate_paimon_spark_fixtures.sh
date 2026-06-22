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
  TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'deletion-vectors.enabled'='true');
INSERT INTO paimon.default.pk_dv VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
UPDATE paimon.default.pk_dv SET v='B2' WHERE id=2;
DELETE FROM paimon.default.pk_dv WHERE id=4;
INSERT INTO paimon.default.pk_dv VALUES (6,'f');

CREATE TABLE paimon.default.pk_dynbucket (id INT, v STRING)
  TBLPROPERTIES ('primary-key'='id', 'bucket'='-1');
INSERT INTO paimon.default.pk_dynbucket VALUES (1,'x'),(2,'y'),(3,'z');
UPDATE paimon.default.pk_dynbucket SET v='Y2' WHERE id=2;

CREATE TABLE paimon.default.appt_fileindex (id INT, name STRING)
  TBLPROPERTIES ('bucket'='-1', 'file-index.bloom-filter.columns'='name');
INSERT INTO paimon.default.appt_fileindex VALUES (1,'alice'),(2,'bob'),(3,'carol');

-- Merge engines: the reference oracle for these (pypaimon's reader does not implement them).
CREATE TABLE paimon.default.pk_partial (id INT, a INT, b STRING)
  TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'merge-engine'='partial-update');
INSERT INTO paimon.default.pk_partial VALUES (1,10,'x1'),(2,20,CAST(NULL AS STRING)),(3,30,'x3');
INSERT INTO paimon.default.pk_partial VALUES (1,CAST(NULL AS INT),'y1'),(2,CAST(NULL AS INT),'y2'),(3,99,CAST(NULL AS STRING));

CREATE TABLE paimon.default.pk_agg (id INT, s INT, m INT)
  TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'merge-engine'='aggregation',
                 'fields.s.aggregate-function'='sum', 'fields.m.aggregate-function'='max');
INSERT INTO paimon.default.pk_agg VALUES (1,10,10),(2,5,5);
INSERT INTO paimon.default.pk_agg VALUES (1,3,99),(2,7,1);
INSERT INTO paimon.default.pk_agg VALUES (1,2,50);

CREATE TABLE paimon.default.pk_firstrow (id INT, v STRING)
  TBLPROPERTIES ('primary-key'='id', 'bucket'='1', 'merge-engine'='first-row',
                 'changelog-producer'='lookup');
INSERT INTO paimon.default.pk_firstrow VALUES (1,'first1'),(2,'first2'),(3,'first3');
INSERT INTO paimon.default.pk_firstrow VALUES (1,'second1'),(2,'second2');

-- Print Spark's own reads as the cross-check oracle (markers parsed by the caller).
SELECT '@@PARTIAL', id, a, b FROM paimon.default.pk_partial ORDER BY id;
SELECT '@@AGG', id, s, m FROM paimon.default.pk_agg ORDER BY id;
SELECT '@@FIRSTROW', id, v FROM paimon.default.pk_firstrow ORDER BY id;
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
