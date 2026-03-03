"""
Bronze Layer: Bulk raw CSV/ZIP to Delta.
- Manual schema (no inferSchema)
- Partition by ingestion_date only - no expensive timestamp parsing
- Symbol extracted from file path via input_file_name()
"""

import zipfile
from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, input_file_name, regexp_extract

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


def run(spark: Optional[SparkSession] = None) -> None:
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

    # ingestion_date = current date - no timestamp parsing, keep Bronze raw
    df = df.withColumn("ingestion_date", current_date())

    # Drop helper column
    df = df.drop("file_path")

    # Filter out rows where symbol extraction failed
    df = df.filter("symbol != ''")

    df.write.format("delta").partitionBy("ingestion_date").mode("append").save(bronze_path)


if __name__ == "__main__":
    run()
