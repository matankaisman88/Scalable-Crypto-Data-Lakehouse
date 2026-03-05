#!/bin/bash
# Run Gold aggregation job
# Usage: ./scripts/run_gold.sh [ingestion_date] [--skip-optimize]
#   ingestion_date: optional (e.g. 2024-01-01)
#   --skip-optimize: skip OPTIMIZE+Z-order (faster backfills; run optimize separately later)
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Pass all args through to Python (ingestion_date, --skip-optimize, etc.)
GOLD_ARGS=("$@")

MSYS_NO_PATHCONV=1 docker compose -f docker/docker-compose.yml run --rm spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark-app/src/jobs/gold_aggregations.py "${GOLD_ARGS[@]}"
