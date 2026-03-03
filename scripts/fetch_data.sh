#!/bin/bash
# Fetch Binance historical kline data via containerized AWS CLI
# Uses --no-sign-request (no AWS credentials needed for public data)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

# Environment overrides (optional, from host):
#   SYMBOL        (default in compose: BTCUSDT)
#   RESOLUTION    (default in compose: 1m)
#   DATE_PATTERN  (default in compose: 2024-01)
#
# Example:
#   SYMBOL=ETHUSDT RESOLUTION=1h DATE_PATTERN=2024-02 ./scripts/fetch_data.sh
#
# Tip: after changing RESOLUTION, you can remove other resolutions from data/raw
# to avoid Spark processing mixed resolutions. See scripts/cleanup_raw.sh.

MSYS_NO_PATHCONV=1 docker compose -f docker/docker-compose.yml run --rm \
  -e SYMBOL \
  -e RESOLUTION \
  -e DATE_PATTERN \
  ingest

echo "Fetch complete. Raw data in data/raw/"
