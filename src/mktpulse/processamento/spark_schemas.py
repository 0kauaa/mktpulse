"""
schemas explicitos usados na leitura do bronze e na escrita do silver.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

QUOTES_SPARK_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), True),
        StructField("trade_date", DateType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("adjusted_close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("currency", StringType(), True),
        StructField("source", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
        StructField("run_id", StringType(), True),
        StructField("content_hash", StringType(), True),
    ]
)

SILVER_COLUMNS = [
    *[field.name for field in QUOTES_SPARK_SCHEMA.fields],
    "year",
    "month",
    "daily_return",
    "log_return",
    "return_5d",
    "return_20d",
    "volatility_20d",
    "sma_20",
    "sma_50",
    "avg_volume_20d",
    "drawdown_60d",
    "processing_run_id",
    "processed_at",
]
