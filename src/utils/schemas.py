"""
Manual StructTypes for all Medallion layers.
Do NOT use inferSchema - explicit schemas eliminate overhead and ensure consistency.
"""

from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Binance 1s kline CSV has 12 columns (raw format)
BRONZE_RAW_SCHEMA = StructType(
    [
        StructField("open_time", LongType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", DoubleType(), False),
        StructField("close_time", LongType(), False),
        StructField("quote_asset_volume", DoubleType(), False),
        StructField("num_trades", LongType(), False),
        StructField("taker_buy_base", DoubleType(), False),
        StructField("taker_buy_quote", DoubleType(), False),
        StructField("ignore", LongType(), False),
    ]
)

# Bronze: raw + symbol, ingestion_date (from path or current_date - no timestamp parsing)
BRONZE_SCHEMA = StructType(
    list(BRONZE_RAW_SCHEMA.fields)
    + [
        StructField("symbol", StringType(), False),
        StructField("ingestion_date", DateType(), False),
    ]
)

# Silver: deduplicated, casted, with metadata
SILVER_SCHEMA = StructType(
    [
        StructField("open_time", LongType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", DoubleType(), False),
        StructField("close_time", LongType(), False),
        StructField("quote_asset_volume", DoubleType(), False),
        StructField("num_trades", LongType(), False),
        StructField("taker_buy_base", DoubleType(), False),
        StructField("taker_buy_quote", DoubleType(), False),
        StructField("ignore", LongType(), False),
        StructField("symbol", StringType(), False),
        StructField("date", DateType(), False),
        StructField("ingestion_date", DateType(), False),
        StructField("ingestion_ts", TimestampType(), False),
        StructField("coin_name", StringType(), True),
    ]
)

# Gold: 5-min OHLCV
GOLD_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("timestamp", LongType(), False),
        StructField("open", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("volume", DoubleType(), False),
        StructField("num_trades", LongType(), False),
        StructField("date", DateType(), False),
    ]
)

# Coin metadata (small reference table for broadcast join)
METADATA_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("name", StringType(), True),
    ]
)
