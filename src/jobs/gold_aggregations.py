"""
Gold Layer: OHLCV pass-through (follows source resolution).
- Infers Silver granularity from timestamp intervals
- No aggregation: 1s→1s, 1m→1m, 5m→5m
- MERGE only new batch into Gold (preserves Z-ORDER on target)
- OPTIMIZE only newly merged partitions
"""

from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    first,
    floor,
    last,
    to_date,
    to_timestamp,
)
from pyspark.sql.functions import (
    max as spark_max,
)
from pyspark.sql.functions import (
    min as spark_min,
)
from pyspark.sql.functions import (
    sum as spark_sum,
)
from pyspark.sql.types import LongType

from src.quality.quality_checks import validate_silver
from src.utils.config_loader import get_paths, get_gold_window_seconds
from src.utils.spark_session import get_spark_session


def _infer_window_seconds(spark: SparkSession, silver_df) -> int:
    """Infer aggregation window from Silver timestamp intervals (open_time in microseconds)."""
    from pyspark.sql.functions import lag
    from pyspark.sql.window import Window

    w = Window.partitionBy("symbol").orderBy("open_time")
    with_diff = silver_df.withColumn(
        "interval_us", col("open_time") - lag("open_time", 1).over(w)
    ).filter(col("interval_us").isNotNull() & (col("interval_us") > 0))

    if with_diff.isEmpty():
        return get_gold_window_seconds()  # fallback to config

    stats = with_diff.approxQuantile("interval_us", [0.5], 0.01)
    # Single column returns List[float], not List[List[float]]
    median_us = float(stats[0]) if stats else 1_000_000

    median_sec = median_us / 1_000_000
    return max(1, int(round(median_sec)))  # follow source resolution


def run(
    spark: Optional[SparkSession] = None,
    ingestion_date: Optional[str] = None,
    skip_validation: bool = False,
) -> None:
    paths = get_paths()
    silver_path = paths["silver"]
    gold_path = paths["gold"]

    spark = spark or get_spark_session("GoldAggregations")

    # Incremental load: read only new batch from Silver
    silver = spark.read.format("delta").load(silver_path)
    if ingestion_date:
        silver = silver.filter(col("ingestion_date") == ingestion_date)

    # Drop any rows with null open_time or invalid symbol before validation/aggregation
    silver = silver.filter(
        col("open_time").isNotNull() & col("symbol").isNotNull() & (col("symbol") != "")
    )

    if silver.isEmpty():
        return

    # Validate before Gold write (after cleaning obvious bad rows)
    if not skip_validation:
        validate_silver(silver)

    # Auto-detect aggregation window from Silver timestamp intervals
    window_sec = _infer_window_seconds(spark, silver)
    window_us = window_sec * 1_000_000
    silver = silver.withColumn(
        "window_start",
        (floor(col("open_time") / window_us) * window_us).cast(LongType()),
    )

    # OHLCV aggregation
    gold_batch = (
        silver.groupBy("symbol", "window_start")
        .agg(
            first("open").alias("open"),
            spark_max("high").alias("high"),
            spark_min("low").alias("low"),
            last("close").alias("close"),
            spark_sum("volume").alias("volume"),
            spark_sum("num_trades").alias("num_trades"),
        )
        .withColumn("timestamp", col("window_start"))
        .withColumn("date", to_date(to_timestamp(col("window_start") / 1_000_000)))
        .select(
            "symbol",
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "num_trades",
            "date",
        )
    )

    # Incremental MERGE: only merge this batch into Gold
    if DeltaTable.isDeltaTable(spark, gold_path):
        target = DeltaTable.forPath(spark, gold_path)
        target.alias("target").merge(
            gold_batch.alias("source"),
            "target.symbol = source.symbol AND target.timestamp = source.timestamp",
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        gold_batch.write.format("delta").partitionBy("symbol", "date").mode("overwrite").save(
            gold_path
        )

    # OPTIMIZE only newly merged partitions (preserves Z-ORDER on rest of table)
    if DeltaTable.isDeltaTable(spark, gold_path):
        dates = [row.date for row in gold_batch.select("date").distinct().collect()]
        for d in dates:
            d_str = str(d) if d else ""
            if d_str:
                DeltaTable.forPath(spark, gold_path).optimize().where(
                    f"date = '{d_str}'"
                ).executeZOrderBy(["symbol", "timestamp"])


if __name__ == "__main__":
    import sys

    ingestion_date = sys.argv[1] if len(sys.argv) > 1 else None
    run(ingestion_date=ingestion_date)
