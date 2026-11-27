from __future__ import annotations

from datetime import date

import pandas as pd

from mktpulse.indicators import add_indicators
from mktpulse.quality import evaluate_quality
from mktpulse.spark_schemas import QUOTES_SPARK_SCHEMA
from tests.conftest import make_quotes_frame

def _quality_frame(spark, frame: pd.DataFrame):
    bronze = spark.createDataFrame(frame, schema=QUOTES_SPARK_SCHEMA)
    return add_indicators(bronze)

def test_clean_quotes_pass_all_critical_rules(spark) -> None:
    frame = make_quotes_frame("PETR4", days=20)

    report = evaluate_quality(
        _quality_frame(spark, frame), as_of=date(2026, 1, 31), max_staleness_days=5
    )

    assert report.status == "success"
    assert report.total_rows == 20
    assert report.critical_failures == 0
    assert all(rule.passed for rule in report.rules if rule.severity == "critical")

def test_invalid_ohlc_and_duplicate_key_block_the_silver(spark) -> None:
    frame = make_quotes_frame("PETR4", days=5, bad=True)
    frame = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)

    report = evaluate_quality(_quality_frame(spark, frame), as_of=date(2026, 1, 31))
    rules = {rule.name: rule for rule in report.rules}

    assert report.status == "failed"
    assert rules["invalid_ohlc"].failed_rows >= 1
    assert rules["duplicate_business_key"].failed_rows >= 2
    assert rules["duplicate_business_key"].sample
    assert all(not key.startswith("_") for key in rules["duplicate_business_key"].sample[0])

def test_future_trade_date_is_critical(spark) -> None:
    frame = make_quotes_frame("PETR4", days=2)

    report = evaluate_quality(_quality_frame(spark, frame), as_of=date(2026, 1, 1))
    rules = {rule.name: rule for rule in report.rules}

    assert report.status == "failed"
    assert rules["future_trade_date"].failed_rows == 2

def test_stale_symbol_is_only_a_warning(spark) -> None:
    frame = make_quotes_frame("PETR4", days=5)

    report = evaluate_quality(
        _quality_frame(spark, frame), as_of=date(2026, 3, 1), max_staleness_days=5
    )
    rules = {rule.name: rule for rule in report.rules}

    assert report.status == "success"
    assert report.warning_failures >= 5
    assert rules["stale_symbol"].failed_rows == 5
