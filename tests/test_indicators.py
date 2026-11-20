from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from mktpulse.indicators import add_indicators
from mktpulse.spark_schemas import QUOTES_SPARK_SCHEMA


def _spark_quotes(spark, closes: list[float]):
    rows = [
        (
            "PETR4",
            date(2026, 1, 2) + timedelta(days=index),
            close,
            close + 1.0,
            close - 1.0,
            close,
            close,
            1000 + index,
            "BRL",
            "brapi",
            datetime(2026, 11, 18, 12, 0),
            "test-run",
            "a" * 64,
        )
        for index, close in enumerate(closes)
    ]

    return spark.createDataFrame(rows, schema=QUOTES_SPARK_SCHEMA)


def test_daily_return_and_window_min_periods(spark) -> None:
    closes = [100.0 + index for index in range(25)]

    result = add_indicators(_spark_quotes(spark, closes)).orderBy("trade_date").collect()

    assert result[0].daily_return is None
    assert result[1].daily_return == pytest.approx(0.01)
    assert result[0].sma_20 is None
    assert result[18].sma_20 is None
    assert result[19].sma_20 == pytest.approx(sum(closes[:20]) / 20)
    assert result[19].volatility_20d is None
    assert result[20].volatility_20d is not None
    assert result[24].return_5d == pytest.approx(closes[24] / closes[19] - 1.0)
    assert result[24].return_20d == pytest.approx(closes[24] / closes[4] - 1.0)
    assert result[24].log_return == pytest.approx(__import__("math").log(closes[24] / closes[23]))
    assert result[24].sma_50 is None
    assert result[24].drawdown_60d is None


def test_indicators_are_partitioned_by_symbol(spark) -> None:
    first = (100.0, 101.0)
    second = (200.0, 220.0)
    rows = []
    for symbol, closes in (("PETR4", first), ("VALE3", second)):
        for index, close in enumerate(closes):
            rows.append(
                (
                    symbol,
                    date(2026, 1, 2) + timedelta(days=index),
                    close,
                    close + 1.0,
                    close - 1.0,
                    close,
                    close,
                    1000,
                    "BRL",
                    "brapi",
                    datetime(2026, 11, 18, 12, 0),
                    "test-run",
                    "a" * 64,
                )
            )

    frame = spark.createDataFrame(rows, schema=QUOTES_SPARK_SCHEMA)
    result = {
        row.symbol: row
        for row in add_indicators(frame).filter("trade_date = '2026-01-03'").collect()
    }

    assert result["PETR4"].daily_return == pytest.approx(0.01)
    assert result["VALE3"].daily_return == pytest.approx(0.10)
