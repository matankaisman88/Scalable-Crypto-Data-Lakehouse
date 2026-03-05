"""
Pipeline orchestrator for the manual data-refresh UI button.

Steps:
  0. Cleanup — drop old raw files first (start fresh)
  1. Fetch   — download yesterday's raw kline ZIPs from Binance public S3
              directly via boto3 (no Docker daemon / AWS credentials needed).
  2. Bronze  — CSV/ZIP → Delta
  3. Silver  — dedup, cast, metadata join
  4. Gold    — OHLCV aggregation

All Spark stages run in-process (local Spark). No spark-submit, no network
connection to spark-master required.
"""

import os
import shutil
import zipfile
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator, List, Optional, Tuple


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def yesterday() -> str:
    """Return yesterday's date as YYYY-MM-DD (UTC)."""
    return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def _load_symbols() -> List[str]:
    """Read symbols from coin_metadata.csv."""
    import csv

    from src.utils.config_loader import get_paths

    paths = get_paths()
    metadata_path = paths.get("metadata", "/data/metadata")
    csv_path = Path(metadata_path) / "coin_metadata.csv"
    if not csv_path.exists():
        return []
    with open(csv_path, newline="") as fh:
        reader = csv.DictReader(fh)
        return [row["symbol"].strip() for row in reader if row.get("symbol")]


# ─────────────────────────────────────────────────────────────────────────────
# Step 0 — Fetch raw data from Binance public S3
# ─────────────────────────────────────────────────────────────────────────────

_BINANCE_BUCKET = "data.binance.vision"
_KLINES_PREFIX = "data/spot/daily/klines"


def fetch_raw_data(target_date: str) -> Iterator[str]:
    """
    Download kline ZIP files for every symbol in coin_metadata.csv
    from the Binance public S3 bucket (no AWS credentials required).

    Files land at /data/raw/{SYMBOL}/{RESOLUTION}/{SYMBOL}-{RESOLUTION}-{DATE}.zip
    and are extracted in-place so Bronze can read the CSVs.

    Yields progress lines; raises RuntimeError on critical failure.
    """
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    resolution = os.getenv("RESOLUTION", "1m")
    symbols = _load_symbols()

    if not symbols:
        yield "[Fetch] WARNING: No symbols found in coin_metadata.csv — skipping fetch."
        return

    from src.utils.config_loader import get_paths

    paths = get_paths()
    raw_base = Path(paths.get("raw", "/data/raw"))

    s3 = boto3.client(
        "s3",
        config=Config(signature_version=UNSIGNED),
        region_name="us-east-1",
    )

    yield f"[Fetch] Symbols   : {', '.join(symbols)}"
    yield f"[Fetch] Resolution: {resolution}"
    yield f"[Fetch] Date      : {target_date}"
    yield f"[Fetch] Bucket    : s3://{_BINANCE_BUCKET}"

    downloaded = 0
    skipped = 0
    errors = 0

    for symbol in symbols:
        filename = f"{symbol}-{resolution}-{target_date}.zip"
        s3_key = f"{_KLINES_PREFIX}/{symbol}/{resolution}/{filename}"
        local_dir = raw_base / symbol / resolution
        local_zip = local_dir / filename
        local_csv = local_dir / filename.replace(".zip", ".csv")

        if local_csv.exists():
            yield f"[Fetch] {filename} — already extracted, skipping."
            skipped += 1
            continue

        local_dir.mkdir(parents=True, exist_ok=True)

        try:
            yield f"[Fetch] Downloading {symbol}/{resolution}/{filename} …"
            s3.download_file(_BINANCE_BUCKET, s3_key, str(local_zip))

            # Extract the zip in-place
            with zipfile.ZipFile(local_zip, "r") as zf:
                zf.extractall(local_dir)
            local_zip.unlink(missing_ok=True)

            yield f"[Fetch] ✓ {filename} extracted."
            downloaded += 1

        except s3.exceptions.ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("404", "NoSuchKey"):
                yield f"[Fetch] WARNING: {filename} not found on S3 (data may not be published yet)."
                skipped += 1
            else:
                yield f"[Fetch] ERROR downloading {filename}: {exc}"
                errors += 1
        except Exception as exc:  # noqa: BLE001
            yield f"[Fetch] ERROR downloading {filename}: {exc}"
            errors += 1

    yield (
        f"[Fetch] Done — {downloaded} downloaded, "
        f"{skipped} skipped, {errors} errors."
    )


def drop_raw_files() -> Tuple[int, Path]:
    """
    Delete all files under the raw directory. Data is already in Bronze.
    Returns (count_deleted, raw_path_used).
    Uses config path; falls back to project_root/data/raw if config path doesn't exist.
    """
    from src.utils.config_loader import get_paths

    paths = get_paths()
    raw_path = Path(paths.get("raw", "/data/raw"))
    if not raw_path.exists() or not raw_path.is_dir():
        # Fallback for local runs (e.g. streamlit outside Docker)
        raw_path = _project_root() / "data" / "raw"
    if not raw_path.exists() or not raw_path.is_dir():
        return 0, raw_path

    count = 0
    for item in raw_path.iterdir():
        if item.is_file():
            item.unlink()
            count += 1
        elif item.is_dir():
            shutil.rmtree(item)
            count += 1
    return count, raw_path


# ─────────────────────────────────────────────────────────────────────────────
# Public interface
# ─────────────────────────────────────────────────────────────────────────────


def run_refresh(target_date: Optional[str] = None) -> Iterator[str]:
    """
    Full refresh: Fetch → Bronze → Silver → Gold.

    Yields human-readable progress lines throughout.
    Raises RuntimeError on unrecoverable failure.
    """
    ingest_date = target_date or yesterday()

    yield f"[Orchestrator] Target date: {ingest_date}"

    # ── Step 0: Drop old raw files first (start fresh) ────────────────────
    yield ""
    yield "─" * 50
    yield "[0/4] Dropping old raw files …"
    try:
        n, used_path = drop_raw_files()
        yield f"[0/4] Dropped {n} old raw item(s) from {used_path}."
    except Exception as exc:  # noqa: BLE001
        yield f"[0/4] WARNING: Cleanup failed ({exc}). Continuing …"

    # ── Step 1: Fetch raw data ───────────────────────────────────────────
    yield ""
    yield "─" * 50
    yield f"[1/4] Fetching raw data from Binance S3 for {ingest_date} …"
    yield "─" * 50
    try:
        yield from fetch_raw_data(ingest_date)
    except RuntimeError:
        raise
    except Exception as exc:  # noqa: BLE001
        yield f"[1/4] WARNING: Fetch failed ({exc}). Continuing with existing raw data …"

    # ── Shared SparkSession ───────────────────────────────────────────────
    yield ""
    yield "─" * 50
    yield "Initialising SparkSession …"
    try:
        from src.utils.spark_session import get_spark_session

        spark = get_spark_session("CryptoLakehouse-Refresh")
        yield "SparkSession ready."
    except Exception as exc:
        raise RuntimeError(f"Failed to create SparkSession: {exc}") from exc

    # ── Step 2: Bronze ───────────────────────────────────────────────────
    yield ""
    yield "─" * 50
    yield f"[2/4] Bronze ingestion for {ingest_date} …"
    yield "─" * 50
    try:
        from src.jobs.bronze_ingestion import run as bronze_run

        bronze_run(spark=spark, ingestion_date=ingest_date)
        yield "[2/4] Bronze complete."
    except Exception as exc:
        raise RuntimeError(f"Bronze stage failed: {exc}") from exc

    # ── Step 3: Silver ───────────────────────────────────────────────────
    yield ""
    yield "─" * 50
    yield f"[3/4] Silver transformation for {ingest_date} …"
    yield "─" * 50
    try:
        from src.jobs.silver_transformation import run as silver_run

        silver_run(spark=spark, ingestion_date=ingest_date)
        yield "[3/4] Silver complete."
    except Exception as exc:
        raise RuntimeError(f"Silver stage failed: {exc}") from exc

    # ── Step 4: Gold ─────────────────────────────────────────────────────
    yield ""
    yield "─" * 50
    yield f"[4/4] Gold aggregations for {ingest_date} …"
    yield "─" * 50
    try:
        from src.jobs.gold_aggregations import run as gold_run

        gold_run(spark=spark, ingestion_date=ingest_date)
        yield "[4/4] Gold complete."
    except Exception as exc:
        raise RuntimeError(f"Gold stage failed: {exc}") from exc

    yield ""
    yield "─" * 50
    yield f"[Orchestrator] Refresh complete for {ingest_date}."
    yield "─" * 50
