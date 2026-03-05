#!/bin/bash
# Run full Medallion pipeline: Bronze -> Silver -> Gold
# Usage: ./scripts/run_pipeline.sh [ingestion_date] [--skip-optimize]
#   --skip-optimize: skip Gold OPTIMIZE (faster backfills)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

# Parse args: first non-flag is ingestion_date; --skip-optimize passes through to Gold
INGESTION_DATE=""
GOLD_EXTRA=()
for arg in "$@"; do
  if [[ "$arg" == "--skip-optimize" ]]; then
    GOLD_EXTRA+=("$arg")
  elif [[ -z "$INGESTION_DATE" && "$arg" != -* ]]; then
    INGESTION_DATE="$arg"
  fi
done

# Default ingestion_date to today if not provided
INGESTION_DATE="${INGESTION_DATE:-$(date -u +%Y-%m-%d)}"

echo "Using ingestion_date=${INGESTION_DATE}"

echo "Starting Bronze Ingestion..."
./scripts/run_bronze.sh "${INGESTION_DATE}"

echo "Starting Silver Transformation (incremental)..."
./scripts/run_silver.sh "${INGESTION_DATE}"

echo "Starting Gold Aggregations (incremental)..."
./scripts/run_gold.sh "${INGESTION_DATE}" "${GOLD_EXTRA[@]}"

echo "Medallion pipeline completed successfully."

