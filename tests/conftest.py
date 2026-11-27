from __future__ import annotations

import os
import sys
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from mktpulse.config import Settings
from mktpulse.lake import normalized_key, silver_prefix
from mktpulse.normalize import QUOTES_COLUMNS, QUOTES_SCHEMA

B3_TZ = ZoneInfo("America/Sao_Paulo")
FIXED_NOW = datetime(2026, 11, 17, 15, 30, tzinfo=UTC)

def epoch(day: date) -> int:
    return int(datetime.combine(day, time(0), tzinfo=B3_TZ).timestamp())

def quote_row(
    day: date,
    *,
    open_: float = 10.0,
    high: float = 11.0,
    low: float = 9.0,
    close: float = 10.5,
    volume: int = 1000,
    adjusted: float | None = None,
) -> dict[str, Any]:

    return {
        "date": epoch(day),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "adjustedClose": close if adjusted is None else adjusted,
    }

def make_result(
    symbol: str = "PETR4", rows: list[dict[str, Any]] | None = None, *, currency: str = "BRL"
) -> dict[str, Any]:
    if rows is None:
        rows = [quote_row(date(2026, 1, 2)), quote_row(date(2026, 1, 5))]

    return {
        "symbol": symbol,
        "shortName": symbol,
        "currency": currency,
        "historicalDataPrice": rows,
    }

@pytest.fixture
def settings(tmp_path) -> Settings:
    return Settings(
        _env_file=None,
        source="brapi",
        lake_backend="local",
        lake_root=tmp_path / "lake",
        brapi_batch_size=3,
        request_pause_seconds=0,
        symbols="PETR4,VALE3",
    )

def make_quotes_frame(
    symbol: str = "PETR4",
    *,
    days: int = 5,
    start: date = date(2026, 1, 2),
    bad: bool = False,
) -> pd.DataFrame:
    dates = pd.bdate_range(start=start, periods=days).date
    rows: list[dict[str, Any]] = []
    for index, trade_date in enumerate(dates):
        close = 100.0 + index
        open_ = close - 0.5
        high = close + 1.0
        low = open_ - 1.0
        if bad and index == 1:
            high = close - 2.0
        rows.append(
            {
                "symbol": symbol,
                "trade_date": trade_date,
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "adjusted_close": close,
                "volume": 1000 + index,
                "currency": "BRL",
                "source": "brapi",
                "ingested_at": pd.Timestamp("2026-11-18T12:00:00Z"),
                "run_id": "test-run",
                "content_hash": "a" * 64,
            }
        )
    return pd.DataFrame(rows, columns=QUOTES_COLUMNS)

def write_normalized(root: Path, source: str, frame: pd.DataFrame) -> None:
    for symbol, group in frame.groupby("symbol"):
        path = root / normalized_key(source, symbol)
        path.parent.mkdir(parents=True, exist_ok=True)
        group.to_parquet(path, index=False, schema=QUOTES_SCHEMA)

def make_silver_frame(
    symbol: str = "PETR4",
    *,
    days: int = 160,
    anomaly: bool = False,
    seed: int = 7,
) -> pd.DataFrame:
    dates = pd.bdate_range(start="2025-01-02", periods=days)
    rng = np.random.default_rng(seed)
    returns = rng.normal(0.0002, 0.012, days)
    if anomaly:
        returns[120] = 0.30
    close = 100.0 * np.exp(np.cumsum(returns))
    open_ = close * (1.0 + rng.normal(0.0, 0.003, days))
    high = np.maximum(open_, close) * (1.0 + np.abs(rng.normal(0.0, 0.004, days)))
    low = np.minimum(open_, close) * (1.0 - np.abs(rng.normal(0.0, 0.004, days)))
    volume = rng.integers(100_000, 5_000_000, days, dtype=np.int64)
    if anomaly:
        volume[120] *= 8

    frame = pd.DataFrame(
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
            "source": "brapi",
            "ingested_at": pd.Timestamp("2026-11-18T12:00:00Z"),
            "run_id": "test-run",
            "content_hash": "a" * 64,
        }
    )
    frame["daily_return"] = frame["adjusted_close"].pct_change()
    frame["log_return"] = np.log1p(frame["daily_return"])
    frame["return_5d"] = frame["adjusted_close"].pct_change(5)
    frame["return_20d"] = frame["adjusted_close"].pct_change(20)
    frame["volatility_20d"] = frame["daily_return"].rolling(20, min_periods=20).std() * np.sqrt(252)
    frame["sma_20"] = frame["adjusted_close"].rolling(20, min_periods=20).mean()
    frame["sma_50"] = frame["adjusted_close"].rolling(50, min_periods=50).mean()
    frame["avg_volume_20d"] = frame["volume"].rolling(20, min_periods=20).mean()
    frame["drawdown_60d"] = (
        frame["adjusted_close"] / frame["adjusted_close"].rolling(60, min_periods=60).max() - 1.0
    )
    return frame

def write_silver(root: Path, source: str, frame: pd.DataFrame) -> None:
    path = root / silver_prefix(source)
    path.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, partition_cols=["symbol"], index=False)

@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    from mktpulse.spark_session import build_spark_session

    warehouse = tmp_path_factory.mktemp("spark-warehouse")
    session = build_spark_session(warehouse_dir=warehouse, shuffle_partitions=2)
    yield session
    session.stop()
