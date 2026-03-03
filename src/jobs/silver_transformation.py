"""
Silver Layer: Deduplication, casting, metadata join, MERGE (idempotent).
"""

from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, current_timestamp, to_date, to_timestamp
from pyspark.sql.types import LongType

from src.utils.config_loader import get_paths
from src.utils.schemas import METADATA_SCHEMA
from src.utils.spark_session import get_spark_session


def run(spark: Optional[SparkSession] = None, ingestion_date: Optional[str] = None) -> None:
    paths = get_paths()
    bronze_path = paths["bronze"]
    silver_path = paths["silver"]
    metadata_path = paths.get("metadata", "/data/metadata")

    spark = spark or get_spark_session("SilverTransformation")

    # Read Bronze - optionally filter by ingestion_date for incremental
    bronze = spark.read.format("delta").load(bronze_path)
    if ingestion_date:
        bronze = bronze.filter(col("ingestion_date") == ingestion_date)

    # Deduplication on (symbol, open_time)
    bronze = bronze.dropDuplicates(["symbol", "open_time"])

    # Cast: open_time -> timestamp for date derivation
    bronze = bronze.withColumn(
        "date",
        to_date(to_timestamp(col("open_time").cast(LongType()) / 1000)),
    ).withColumn("ingestion_ts", current_timestamp())

    # Load metadata and broadcast join
    metadata_df = spark.read.schema(METADATA_SCHEMA).csv(
        f"{metadata_path}/coin_metadata.csv", header=True
    )
    bronze = (
        bronze.join(
            broadcast(metadata_df),
            bronze.symbol == metadata_df.symbol,
            "left",
        )
        .withColumnRenamed("name", "coin_name")
        .drop(metadata_df.symbol)
    )

    # Select columns for Silver
    silver_cols = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "num_trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore",
        "symbol",
        "date",
        "ingestion_date",
        "ingestion_ts",
        "coin_name",
    ]
    silver = bronze.select(*silver_cols)

    # MERGE (idempotent)
    if DeltaTable.isDeltaTable(spark, silver_path):
        target = DeltaTable.forPath(spark, silver_path)
        target.alias("target").merge(
            silver.alias("source"),
            "target.symbol = source.symbol AND target.open_time = source.open_time",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        silver.write.format("delta").partitionBy("symbol", "date").mode("overwrite").save(
            silver_path
        )


if __name__ == "__main__":
    import sys

    ingestion_date = sys.argv[1] if len(sys.argv) > 1 else None
    run(ingestion_date=ingestion_date)
