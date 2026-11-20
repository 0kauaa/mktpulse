from __future__ import annotations

from mktpulse.benchmark import generate_synthetic_quotes, run_benchmark
from mktpulse.normalize import QUOTES_COLUMNS


def test_synthetic_quotes_match_the_quotes_contract() -> None:
    frame = generate_synthetic_quotes(rows=600, symbol_count=3)

    assert list(frame.columns) == QUOTES_COLUMNS
    assert len(frame) >= 600
    assert frame["symbol"].nunique() == 3
    assert (frame["high"] >= frame[["open", "close"]].max(axis=1)).all()
    assert (frame["low"] <= frame[["open", "close"]].min(axis=1)).all()
    assert (frame["volume"] > 0).all()


def test_benchmark_compares_pandas_and_spark(spark) -> None:
    result = run_benchmark(rows=600, symbol_count=2, shuffle_partitions=2, spark=spark)

    assert result["pandas"]["rows"] == result["spark"]["rows"] >= 600
    assert result["pandas"]["seconds"] >= 0
    assert result["spark"]["seconds"] >= 0
    assert result["pandas_speedup"] >= 0
