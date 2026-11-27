"""
breve comparacao controlada entre pandas e spark com dados sinteticos.
"""

from __future__ import annotations

import tempfile
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from mktpulse.processamento import (
    indicators as processamento_indicators,
    normalize as processamento_normalize,
    spark_schemas as processamento_spark_schemas,
    spark_session as processamento_spark_session,
)

@dataclass
class EngineMetrics:
    engine: str
    rows: int
    seconds: float
    rows_per_second: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def generate_synthetic_quotes(*, rows: int, symbol_count: int, seed: int = 42) -> pd.DataFrame:
    if rows < 60:
        raise ValueError("rows deve ser pelo menos 60 para exercitar as janelas")
    if symbol_count < 1:
        raise ValueError("symbol_count deve ser pelo menos 1")

    days = max(60, rows // symbol_count)
    dates = pd.bdate_range(end="2026-11-16", periods=days).date
    rng = np.random.default_rng(seed)
    frames: list[pd.DataFrame] = []

    for index in range(1, symbol_count + 1):
        symbol = f"SYN{index:02d}"
        returns = rng.normal(0.0005, 0.01, days)
        close = 100.0 * np.exp(np.cumsum(returns))
        open_ = close * (1.0 + rng.normal(0.0, 0.004, days))
        high = np.maximum(open_, close) * (1.0 + np.abs(rng.normal(0.0, 0.004, days)))
        low = np.minimum(open_, close) * (1.0 - np.abs(rng.normal(0.0, 0.004, days)))
        volume = rng.integers(1_000, 10_000_000, days, dtype=np.int64)

        frames.append(
            pd.DataFrame(
                {
                    "symbol": symbol,
                    "trade_date": dates,
                    "open": open_,
                    "high": high,
                    "low": low,
                    "close": close,
                    "adjusted_close": close,
                    "volume": volume,
                    "currency": "BRL",
                    "source": "synthetic",
                    "ingested_at": pd.Timestamp("2026-11-18T12:00:00Z"),
                    "run_id": "benchmark",
                    "content_hash": "0" * 64,
                }
            )
        )

    return pd.concat(frames, ignore_index=True)[processamento_normalize.QUOTES_COLUMNS]


def _pandas_metrics(path: Path, output: Path) -> EngineMetrics:
    start = time.perf_counter()
    frame = pd.read_parquet(path)
    frame = frame.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    grouped = frame.groupby("symbol", group_keys=False)

    frame["daily_return"] = grouped["adjusted_close"].pct_change()
    frame["log_return"] = np.log1p(frame["daily_return"])
    frame["return_5d"] = grouped["adjusted_close"].pct_change(5)
    frame["return_20d"] = grouped["adjusted_close"].pct_change(20)
    frame["volatility_20d"] = grouped["daily_return"].transform(
        lambda values: values.rolling(20, min_periods=20).std() * np.sqrt(252)
    )
    frame["sma_20"] = grouped["adjusted_close"].transform(
        lambda values: values.rolling(20, min_periods=20).mean()
    )
    frame["sma_50"] = grouped["adjusted_close"].transform(
        lambda values: values.rolling(50, min_periods=50).mean()
    )
    frame["avg_volume_20d"] = grouped["volume"].transform(
        lambda values: values.rolling(20, min_periods=20).mean()
    )
    frame["drawdown_60d"] = grouped["adjusted_close"].transform(
        lambda values: values / values.rolling(60, min_periods=60).max() - 1.0
    )
    frame.to_parquet(output, index=False)

    seconds = time.perf_counter() - start
    return EngineMetrics(
        engine="pandas",
        rows=len(frame),
        seconds=round(seconds, 3),
        rows_per_second=round(len(frame) / seconds, 1) if seconds else 0.0,
    )


def _spark_metrics(
    spark: SparkSession,
    path: Path,
    output: Path,
) -> EngineMetrics:
    start = time.perf_counter()
    frame = spark.read.schema(processamento_spark_schemas.QUOTES_SPARK_SCHEMA).parquet(str(path))
    prepared = processamento_indicators.add_indicators(frame).repartition("symbol").sortWithinPartitions("trade_date")
    prepared.write.mode("overwrite").parquet(str(output))
    rows = spark.read.parquet(str(output)).count()
    seconds = time.perf_counter() - start

    return EngineMetrics(
        engine="spark",
        rows=rows,
        seconds=round(seconds, 3),
        rows_per_second=round(rows / seconds, 1) if seconds else 0.0,
    )


def run_benchmark(
    *,
    rows: int = 250_000,
    symbol_count: int = 8,
    shuffle_partitions: int = 8,
    spark: SparkSession | None = None,
) -> dict[str, Any]:
    session = spark
    owned = False
    if session is None:
        session = processamento_spark_session.build_spark_session(
            app_name="mktpulse-benchmark", shuffle_partitions=shuffle_partitions
        )
        owned = True

    try:
        with tempfile.TemporaryDirectory(prefix="mktpulse-benchmark-") as temp_dir:
            root = Path(temp_dir)
            source = root / "quotes.parquet"
            pandas_output = root / "pandas"
            spark_output = root / "spark"

            frame = generate_synthetic_quotes(rows=rows, symbol_count=symbol_count)
            frame.to_parquet(source, index=False, schema=processamento_normalize.QUOTES_SCHEMA)

            pandas_metrics = _pandas_metrics(source, pandas_output)
            spark_metrics = _spark_metrics(session, source, spark_output)

            pandas_speedup = (
                round(spark_metrics.seconds / pandas_metrics.seconds, 3)
                if pandas_metrics.seconds
                else 0.0
            )

            return {
                "rows_requested": rows,
                "rows": len(frame),
                "symbols": symbol_count,
                "shuffle_partitions": shuffle_partitions,
                "pandas": pandas_metrics.to_dict(),
                "spark": spark_metrics.to_dict(),
                "pandas_speedup": pandas_speedup,
            }

    finally:
        if owned:
            session.stop()
