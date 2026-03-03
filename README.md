# Scalable Crypto Data Lakehouse

Production-grade Medallion Architecture data lakehouse for Binance Vision historical kline data. Built with Spark 3.5, Delta Lake, and Great Expectations, designed to run under modest resource constraints (default 4GB worker, 1GB executor; tunable via config).

## Architecture

```
Binance S3 → Ingest (aws-cli) → Raw CSV/ZIP
    → Bronze (Delta, partition by ingestion_date)
    → Silver (Dedupe, broadcast join, MERGE)
    → GX Validation
    → Gold (5-min OHLCV, incremental MERGE, OPTIMIZE Z-ORDER)
```

### Medallion Layers

| Layer | Purpose | Partitioning |
|-------|---------|--------------|
| Bronze | Raw ingestion, minimal transform | `ingestion_date` |
| Silver | Deduplication, metadata join, MERGE | `symbol`, `date` |
| Gold | 5-min OHLCV, incremental MERGE | `symbol`, `date` |

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

### 2. Start Spark Cluster

```bash
docker compose -f docker/docker-compose.yml up -d spark-master spark-worker history-server
```

### 3. Run Pipeline

#### Git Bash / WSL / Linux / macOS

```bash
./scripts/run_bronze.sh              # Bronze only (Delta-enabled, Git Bash friendly)
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
PYTHONPATH=. python -m src.jobs.bronze_ingestion
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

- **Access the UI** at `http://localhost:8501` to explore 5-minute OHLCV candlesticks from the Gold Delta table with symbol/date filters, a dark-themed chart, and basic metrics.

## Project Structure

```
/config          config.yaml
/docker          Dockerfile, docker-compose.yml
/src
  /jobs         bronze_ingestion, silver_transformation, gold_aggregations
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
