"""
Unit tests for Gold OHLCV aggregation logic.
"""

from pyspark.sql.functions import (
    col,
    first,
    floor,
    last,
    lit,
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

# 5 minutes in microseconds (matches Binance Vision bulk CSV format)
WINDOW_US = 5 * 60 * 1_000_000


def _aggregate_ohlcv(df):
    """Same aggregation logic as gold_aggregations.py."""
    df = df.withColumn(
        "window_start",
        (floor(col("open_time") / WINDOW_US) * WINDOW_US).cast(LongType()),
    )
    return (
        df.groupBy("symbol", "window_start")
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
    )


def test_gold_ohlcv_aggregation(spark_session):
    """Verify 5-min OHLCV: first(open), max(high), min(low), last(close), sum(volume)."""
    # 10 rows at 1s intervals (microseconds) -> all in same 5-min window
    data = [
        (
            1_000_000 * i,
            100.0 + i,
            101.0 + i,
            99.0 - i,
            100.5 + i,
            10.0,
            1_000_000 * i + 999_999,
            1000.0,
            5,
            5.0,
            500.0,
            0,
        )
        for i in range(10)
    ]
    columns = [
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
    ]
    df = spark_session.createDataFrame(data, columns).withColumn("symbol", lit("BTCUSDT"))

    result = _aggregate_ohlcv(df)

    rows = result.collect()
    assert len(rows) == 1
    r = rows[0]
    assert r.open == 100.0  # first(open)
    assert r.high == 110.0  # max(101+i) = 101+9
    assert r.low == 90.0  # min(99-i) = 99-9
    assert r.close == 109.5  # last(close) = 100.5+9
    assert r.volume == 100.0  # sum(10) over 10 rows
    assert r.num_trades == 50  # sum(5) over 10 rows


def test_ohlcv_aggregation_logic():
    """Pure Python sanity check for OHLCV aggregation (no Spark)."""
    # Simulate: 10 rows, open=100+i, high=101+i, low=99-i, close=100.5+i, volume=10
    opens = [100.0 + i for i in range(10)]
    highs = [101.0 + i for i in range(10)]
    lows = [99.0 - i for i in range(10)]
    closes = [100.5 + i for i in range(10)]
    volumes = [10.0] * 10
    # first(open)=100, max(high)=110, min(low)=90, last(close)=109.5, sum(volume)=100
    assert opens[0] == 100.0
    assert max(highs) == 110.0
    assert min(lows) == 90.0
    assert closes[-1] == 109.5
    assert sum(volumes) == 100.0
