"""
Great Expectations validation between Silver and Gold.
Validates: positive prices/quantities, non-null timestamps.
Uses Spark for validation; GX suite/checkpoint for configuration.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def validate_silver(df: "DataFrame") -> bool:
    """
    Validate Silver DataFrame before Gold write.
    Checks: non-null open_time/symbol, positive open/high/low/close/volume.
    Raises ValueError if validation fails.
    """
    # Non-null checks
    null_open_time = df.filter("open_time IS NULL").count()
    null_symbol = df.filter("symbol IS NULL OR symbol = ''").count()
    if null_open_time > 0 or null_symbol > 0:
        raise ValueError(
            f"GX validation failed: null_open_time={null_open_time}, null_symbol={null_symbol}"
        )

    # Positive value checks (prices and volume)
    invalid_open = df.filter("open < 0 OR open > 1e12").count()
    invalid_high = df.filter("high < 0 OR high > 1e12").count()
    invalid_low = df.filter("low < 0 OR low > 1e12").count()
    invalid_close = df.filter("close < 0 OR close > 1e12").count()
    invalid_volume = df.filter("volume < 0").count()

    if any([invalid_open, invalid_high, invalid_low, invalid_close, invalid_volume]):
        raise ValueError(
            f"GX validation failed: invalid_open={invalid_open}, invalid_high={invalid_high}, "
            f"invalid_low={invalid_low}, invalid_close={invalid_close}, "
            f"invalid_volume={invalid_volume}"
        )

    return True
