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

**Z-Order (timestamp only)**

- `OPTIMIZE ... ZORDER BY (timestamp)` improves predicate pushdown (partition columns like `symbol` cannot be Z-ordered)
- Run only on newly merged partitions to avoid full-table rewrites

**MERGE Idempotency**

- Silver: match on `(symbol, open_time)` — re-runs do not create duplicates
- Gold: match on `(symbol, timestamp)` — incremental load preserves target Z-ORDER

**Binance Vision timestamp format**

- Raw kline CSVs use `open_time` in **microseconds** (not milliseconds). Silver/Gold derive dates and windows from source resolution.

**History Server**

- Event logs: `http://localhost:18080`
- Debug slow stages, skew, spills via Spark UI

## Analysis: Silver vs Gold

### Silver Layer (`silver_klines`)

Silver holds **cleaned, deduplicated raw klines** at source resolution (1s, 1m, or 5m). It includes:

- **OHLCV + metadata**: `open`, `high`, `low`, `close`, `volume`, `num_trades`
- **Extra columns** (not in Gold): `coin_name`, `close_time`, `quote_asset_volume`, `taker_buy_base`, `taker_buy_quote`
- **Partitioning**: `symbol`, `date`

**When to use Silver:** When you need `quote_asset_volume`, `taker_buy_base`, `taker_buy_quote`, `coin_name`, or `close_time`. For standard OHLCV analytics, prefer Gold.

### Gold Layer (`gold_ohlcv`)

Gold holds **aggregated OHLCV** at the same resolution as Silver (1s→1s, 1m→1m, 5m→5m). It includes:

- **Analytics-ready schema**: `symbol`, `timestamp`, `open`, `high`, `low`, `close`, `volume`, `num_trades`, `date`
- **Optimized for queries**: Z-ordered by `timestamp`, partition pruning by `symbol` and `date`
- **Partitioning**: `symbol`, `date`

**When to use Gold:** For most analytics — price, volume, trades, candlestick analysis. Gold is the default for the Dashboard and AI Query.

| Need                          | Use   |
|-------------------------------|-------|
| OHLCV, volume, num_trades     | Gold  |
| quote_asset_volume            | Silver|
| taker_buy_base / taker_buy_quote | Silver |
| coin_name                     | Silver|
| close_time                    | Silver|

## Run Modes

Two ways to ingest and process data:

| Mode | Use case | How |
|------|----------|-----|
| **1. Manual Pipeline** | Scripts, CI/CD, custom dates | Fetch → Start Spark cluster → Run `run_pipeline.sh` |
| **2. Dashboard** | Interactive UI, daily refresh, backfill | Start dashboard → Use **Refresh Yesterday** or **Advanced: Manual Backfill** |

---

## Mode 1: Manual Pipeline

For running the pipeline via scripts (e.g. cron, CI, or one-off backfills).

### Prerequisites

- Docker & Docker Compose
- Python 3.10+ (for local dev)

### Step 1: Fetch Data

```bash
./scripts/fetch_data.sh
```

Override defaults via environment variables:

```bash
SYMBOL=BTCUSDT RESOLUTION=1m DATE_PATTERN=2024-01 ./scripts/fetch_data.sh
```

**1s ingestion:** `RESOLUTION=1s DATE_PATTERN=2026 ./scripts/fetch_data.sh`

### Step 2: Start Spark Cluster

```bash
docker compose -f docker/docker-compose.yml up -d spark-master spark-worker history-server
```

### Step 3: Run Pipeline

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

### Step 4: History Server

Open `http://localhost:18080` after job completion.

---

## Mode 2: Dashboard

For interactive exploration, daily refresh, and manual backfill via the UI.

### Start the Dashboard

```bash
docker compose -f docker/docker-compose.yml up -d dashboard
```

Access at `http://localhost:8501`. The dashboard runs fetch + Bronze → Silver → Gold **in-process** (no separate Spark cluster required for refresh).

### Refresh Data (sidebar)

1. **Refresh Yesterday's Data** — One-click pipeline for yesterday. Fast path for daily updates.
2. **Advanced: Manual Backfill** — Date picker to run the pipeline for any past date (up to yesterday).

Both run Fetch → Bronze → Silver → Gold. Cache is cleared on success. Pipeline logs in "View pipeline log" expander.

### Dashboard UI (after at least one Gold job)

![Dashboard](assets/dashboard.png?v=2)

- **Metrics**: Symbol Price, Total Volume, 24h Price Change (formatted with $ and K/M/B suffixes).
- **Candlestick chart**: OHLCV with volume bars; timeframe selector (1m, 1H, 4H, 1D, 1W); current price indicator.
- **Filters**: symbol, date range, price range, min volume, table sort. **Download as CSV**.
- **Memory-optimized**: Predicate pushdown; symbols from `coin_metadata.csv`; 30-day guardrail for 1s data.

### AI Query (Natural Language to SQL)

![AI Query](assets/ai-query.png)

Ask questions in plain English; an LLM translates to Spark SQL and explains results.

- **Requirements**: `OPENAI_API_KEY` in `.env`. Optional: `OPENAI_MODEL` (default `gpt-4o-mini`).
- **Gold vs Silver**: Gold for OHLCV/volume/trades. Silver for `quote_asset_volume`, `taker_buy_base`, `taker_buy_quote`, `coin_name`, `close_time`.
- **Safety**: Only `SELECT`; `LIMIT 100`; must filter by `symbol` and `date`.
- **Timestamps**: `open_time` and `timestamp` are in **microseconds** — use `FROM_UNIXTIME(col/1000000)`.

---

## Configuration

- **config/config.yaml** — Paths, Spark settings, GX checkpoint
- **.env** — Overrides (copy from `.env.example`)
- **.streamlit/config.toml** — Dashboard theme (dark sidebar, light main, dark code blocks)

### Spark Tuning (AQE & Memory)

- `spark.sql.adaptive.enabled=true` — AQE coalesces shuffle partitions at runtime.
- `spark.sql.shuffle.partitions=200` — High initial count; AQE reduces to optimal size.
- Default executor memory is `1g` (see `config/config.yaml` and `docker-compose.yml`); for larger full-history runs (especially Gold), you may want to:
  - Increase `spark.executor.memory` in `config/config.yaml` (for example `2g`–`4g`, depending on host RAM), **and**
  - Ensure `SPARK_WORKER_MEMORY` / container memory limits are set higher than executor memory.
- Alternatively, run Silver/Gold incrementally by passing an `ingestion_date` argument so they operate on the latest batch instead of the entire history, which reduces memory pressure.

## Project Structure

```
/config          config.yaml
/.streamlit      config.toml (dashboard theme)
/docker          Dockerfile, docker-compose.yml
/src
  /jobs         bronze_ingestion, silver_transformation, gold_aggregations
  /dashboard    Streamlit app (OHLCV chart, AI Query tab, Manual Refresh)
  /utils        schemas, spark_session, config_loader, ai_query_helper, pipeline_orchestrator
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
