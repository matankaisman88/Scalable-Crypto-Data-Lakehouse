# Scalable Crypto Data Lakehouse

Production-grade Medallion Architecture data lakehouse for Binance Vision historical kline data. Built with Spark 3.5, Delta Lake, and Great Expectations, designed to run under modest resource constraints (default 4GB worker, 1GB executor; tunable via config).

## Architecture

```
Binance S3 → Ingest (aws-cli) → Raw CSV/ZIP
    → Bronze (Delta, partition by ingestion_date)
    → Silver (Dedupe, broadcast join, MERGE)
    → GX Validation
    → Gold (OHLCV, resolution follows source: 1s→1s, 1m→1m, 5m→5m; incremental MERGE, OPTIMIZE Z-ORDER)
```

### Medallion Layers

| Layer | Purpose | Partitioning |
|-------|---------|--------------|
| Bronze | Raw ingestion, minimal transform | `ingestion_date` |
| Silver | Deduplication, metadata join, MERGE | `symbol`, `date` |
| Gold | OHLCV (1s/1m/5m from source), incremental MERGE | `symbol`, `date` |

**Quality checks** — Silver data is validated (non-null timestamps/symbols, positive prices/volume) inside the Gold job before aggregation; no separate pipeline step.

### Key Design Decisions

**Partitioning (symbol + date)**

- Aligns with common query patterns (filter by symbol, date range)
- Keeps partition count manageable; avoids small-file problem

**Z-Order (symbol, timestamp)**

- `OPTIMIZE ... ZORDER BY (symbol, timestamp)` improves predicate pushdown
- Run only on newly merged partitions to avoid full-table rewrites

**MERGE Idempotency**

- Silver: match on `(symbol, open_time)` — re-runs do not create duplicates
- Gold: match on `(symbol, timestamp)` — incremental load preserves target Z-ORDER

**Binance Vision timestamp format**

- Raw kline CSVs use `open_time` in **microseconds** (not milliseconds). Silver/Gold derive dates and windows from source resolution.

**History Server**

- Event logs: `http://localhost:18080`
- Debug slow stages, skew, spills via Spark UI

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+ (for local dev)

### 1. Fetch Data

```bash
./scripts/fetch_data.sh
```

Optionally override defaults via environment variables, for example:

```bash
SYMBOL=BTCUSDT RESOLUTION=1m DATE_PATTERN=2024-01 ./scripts/fetch_data.sh
```

**1s ingestion** (for 2026):

1. Fetch 1s data: `RESOLUTION=1s DATE_PATTERN=2026 ./scripts/fetch_data.sh`
2. Run pipeline: `./scripts/run_pipeline.sh` — Gold follows source resolution (1s→1s, 1m→1m)
3. Dashboard presents detected resolution at `http://localhost:8501`

### 2. Start Spark Cluster

```bash
docker compose -f docker/docker-compose.yml up -d spark-master spark-worker history-server
```

### 3. Run Pipeline

#### Git Bash / WSL / Linux / macOS

```bash
./scripts/run_bronze.sh [YYYY-MM-DD] # Bronze only (optional ingestion_date; defaults to today)
./scripts/run_silver.sh [YYYY-MM-DD] # Silver only (optional ingestion_date)
./scripts/run_gold.sh   [YYYY-MM-DD] # Gold only (optional ingestion_date)

./scripts/run_pipeline.sh            # Bronze -> Silver -> Gold (incremental by default)
./scripts/run_pipeline.sh 2024-01-01 # Bronze, then Silver/Gold for a specific ingestion_date
```

#### PowerShell

```powershell
# Recommended: call the bash scripts via Git Bash (handles MSYS_NO_PATHCONV and Delta packages):
#   & "C:\Program Files\Git\bin\bash.exe" ./scripts/run_bronze.sh
#   & "C:\Program Files\Git\bin\bash.exe" ./scripts/run_silver.sh
#   & "C:\Program Files\Git\bin\bash.exe" ./scripts/run_gold.sh
#   & "C:\Program Files\Git\bin\bash.exe" ./scripts/run_pipeline.sh
```

Or run Bronze locally (without Docker) with:

```bash
PYTHONPATH=. python -m src.jobs.bronze_ingestion [YYYY-MM-DD]
```

### 4. View History Server

Open `http://localhost:18080` after job completion.

## Configuration

- **config/config.yaml** — Paths, Spark settings, GX checkpoint
- **.env** — Overrides (copy from `.env.example`)

### Spark Tuning (AQE & Memory)

- `spark.sql.adaptive.enabled=true` — AQE coalesces shuffle partitions at runtime.
- `spark.sql.shuffle.partitions=200` — High initial count; AQE reduces to optimal size.
- Default executor memory is `1g` (see `config/config.yaml` and `docker-compose.yml`); for larger full-history runs (especially Gold), you may want to:
  - Increase `spark.executor.memory` in `config/config.yaml` (for example `2g`–`4g`, depending on host RAM), **and**
  - Ensure `SPARK_WORKER_MEMORY` / container memory limits are set higher than executor memory.
- Alternatively, run Silver/Gold incrementally by passing an `ingestion_date` argument so they operate on the latest batch instead of the entire history, which reduces memory pressure.

## Dashboard

- **Start the dashboard** (after running at least one Gold job so the Gold table is populated):

```bash
docker compose -f docker/docker-compose.yml up -d dashboard
```

- **Access the UI** at `http://localhost:8501` to explore OHLCV candlesticks from the Gold Delta table. The dashboard infers resolution (1s, 1m, 5m) from the data and displays it in the title. Features a light, modern design with high-contrast colors.

- **Memory-optimized loading** — Uses predicate pushdown on the Gold Delta table: symbols come from `data/metadata/coin_metadata.csv` (no full-table scan), and data is loaded only for the selected symbol and date range. Date range is chosen *before* load (default: last 7 days). A 30-day guardrail prevents OOM when loading 1s data on 4GB executors.

- **Filters**: symbol, date range (required before load), price range, min volume, and table sort order. Includes basic metrics, candlestick chart, and **Download as CSV** to export filtered data.

## Project Structure

```
/config          config.yaml
/docker          Dockerfile, docker-compose.yml
/src
  /jobs         bronze_ingestion, silver_transformation, gold_aggregations
  /dashboard   Streamlit app for Gold OHLCV visualization
  /utils        schemas, spark_session, config_loader
  /quality      quality_checks, GX suites
/scripts        fetch_data.sh, cleanup_raw.sh, run_bronze.sh, run_pipeline.sh
/tests          conftest, test_transformations
/data           bronze, silver, gold, raw, spark-events
```

## Testing

```bash
pip install -r requirements.txt
pytest tests/ -v
```

## CI

`.github/workflows/basic_ci.yml` — Lint (ruff) and tests (pytest).
