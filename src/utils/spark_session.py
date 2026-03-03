"""
SparkSession builder with Delta Lake, History Server, and AQE tuning.
All config from config_loader - no hardcoded values.
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from .config_loader import get_paths, get_spark_config


def get_spark_session(app_name: str = "CryptoLakehouse") -> SparkSession:
    """Build SparkSession with Delta, event logging, and resource tuning."""
    cfg = get_spark_config()
    paths = get_paths()

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.executor.memory", cfg.get("executor_memory", "1g"))
        .config("spark.memory.fraction", str(cfg.get("memory_fraction", 0.6)))
        .config("spark.memory.storageFraction", str(cfg.get("memory_storage_fraction", 0.5)))
        .config("spark.sql.shuffle.partitions", str(cfg.get("shuffle_partitions", 200)))
        .config("spark.sql.adaptive.enabled", str(cfg.get("adaptive_enabled", True)).lower())
        .config(
            "spark.serializer", cfg.get("serializer", "org.apache.spark.serializer.KryoSerializer")
        )
    )

    if cfg.get("event_log_enabled"):
        builder = builder.config("spark.eventLog.enabled", "true").config(
            "spark.eventLog.dir",
            cfg.get("event_log_dir", paths.get("spark_events", "/opt/spark-events")),
        )

    # Configure Spark with Delta Lake using the pip-installed delta-spark package.
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark
