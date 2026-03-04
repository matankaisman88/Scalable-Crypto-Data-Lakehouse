#!/bin/bash
# Run full Medallion pipeline: Bronze -> Silver -> Gold

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

# Optional first argument: ingestion_date (e.g. 2024-01-01).
# If not provided, default to today's date in UTC.
INGESTION_DATE="${1:-$(date -u +%Y-%m-%d)}"

echo "Using ingestion_date=${INGESTION_DATE}"

echo "Starting Bronze Ingestion..."
./scripts/run_bronze.sh "${INGESTION_DATE}"

echo "Starting Silver Transformation (incremental)..."
./scripts/run_silver.sh "${INGESTION_DATE}"

echo "Starting Gold Aggregations (incremental)..."
./scripts/run_gold.sh "${INGESTION_DATE}"

echo "Medallion pipeline completed successfully."

