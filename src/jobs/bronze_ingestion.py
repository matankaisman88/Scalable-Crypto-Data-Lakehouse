"""
Bronze Layer: Bulk raw CSV/ZIP to Delta.
- Manual schema (no inferSchema)
- Partition by ingestion_date only - no expensive timestamp parsing
- Symbol extracted from file path via input_file_name()
- Optional ingestion_date arg: use specific date for batch (e.g. 2024-01-01), else current_date
"""

import sys
import zipfile
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, input_file_name, lit, regexp_extract, to_date

from src.utils.config_loader import get_paths
from src.utils.schemas import BRONZE_RAW_SCHEMA
from src.utils.spark_session import get_spark_session


def _extract_zips(raw_path: str) -> None:
    """Extract ZIP files in place. Keeps Bronze ingestion simple."""
    raw = Path(raw_path)
    if not raw.exists():
        return
    for zf in raw.rglob("*.zip"):
        try:
            with zipfile.ZipFile(zf, "r") as z:
                z.extractall(zf.parent)
        except zipfile.BadZipFile:
            pass


def run(spark: Optional[SparkSession] = None, ingestion_date: Optional[str] = None) -> None:
    paths = get_paths()
    raw_path = paths["raw"]
    bronze_path = paths["bronze"]

    _extract_zips(raw_path)

    spark = spark or get_spark_session("BronzeIngestion")

    # Read all CSV with manual schema (recursive under raw_path)
    df = (
        spark.read.schema(BRONZE_RAW_SCHEMA)
        .option("header", "false")
        .option("recursiveFileLookup", "true")
        .csv(raw_path)
    )

    # Add symbol from path (e.g. .../raw/BTCUSDT/1s/file.csv -> BTCUSDT)
    # Local/inside-container path format: .../raw/SYMBOL/RESOLUTION/SYMBOL-RESOLUTION-*.csv
    df = df.withColumn(
        "file_path",
        input_file_name(),
    ).withColumn(
        "symbol",
        regexp_extract("file_path", r"/raw/([^/]+)/", 1),
    )

    # ingestion_date: use arg if provided (e.g. 2024-01-01 for pipeline), else current date
    if ingestion_date:
        df = df.withColumn("ingestion_date", to_date(lit(ingestion_date)))
    else:
        df = df.withColumn("ingestion_date", current_date())

    # Drop helper column
    df = df.drop("file_path")

    # Filter out rows where symbol extraction failed
    df = df.filter("symbol != ''")

    df.write.format("delta").partitionBy("ingestion_date").mode("append").save(bronze_path)


if __name__ == "__main__":
    ingestion_date = sys.argv[1] if len(sys.argv) > 1 else None
    run(ingestion_date=ingestion_date)
