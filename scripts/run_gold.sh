#!/bin/bash
# Run Gold aggregation job
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Optional first argument: ingestion_date (e.g. 2024-01-01)
INGESTION_DATE="${1:-}"

MSYS_NO_PATHCONV=1 docker compose -f docker/docker-compose.yml run --rm spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark-app/src/jobs/gold_aggregations.py ${INGESTION_DATE:+$INGESTION_DATE}
