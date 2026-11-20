from __future__ import annotations

import pandas as pd
import pytest

from dashboard.data import (
    filter_period,
    lake_paths,
    merge_anomalies,
    summarize_symbols,
    top_anomalies,
)


def _silver() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["PETR4", "PETR4", "VALE3"],
            "trade_date": pd.to_datetime(["2026-10-24", "2026-10-26", "2026-10-26"]),
            "close": [30.0, 21.0, 60.0],
            "adjusted_close": [30.0, 21.0, 60.0],
            "daily_return": [0.0, -0.30, 0.01],
            "volatility_20d": [0.30, 0.42, 0.25],
            "volume_ratio_20d": [0.1, 0.8, 0.2],
            "is_anomaly": [False, True, False],
            "anomaly_score": [0.1, 0.9, 0.2],
        }
    )


def test_merge_anomalies_keeps_silver_rows_and_flags() -> None:
    silver = _silver().drop(columns=["is_anomaly", "anomaly_score"])
    gold = _silver()[["symbol", "trade_date", "is_anomaly", "anomaly_score"]]

    merged = merge_anomalies(silver, gold)

    assert len(merged) == 3
    assert merged.loc[merged["trade_date"] == "2026-10-26", "is_anomaly"].sum() == 1


def test_merge_anomalies_without_gold_marks_everything_false() -> None:
    merged = merge_anomalies(
        _silver().drop(columns=["is_anomaly", "anomaly_score"]), pd.DataFrame()
    )

    assert not merged["is_anomaly"].any()
    assert merged["anomaly_score"].isna().all()


def test_summary_and_top_anomalies() -> None:
    frame = _silver()

    summary = summarize_symbols(frame)
    top = top_anomalies(frame)

    petr4 = summary.loc[summary["symbol"] == "PETR4"].iloc[0]
    assert petr4["ultimo_pregao"].isoformat() == "2026-10-26"
    assert petr4["anomalias"] == 1
    assert len(top) == 1
    assert top.iloc[0]["symbol"] == "PETR4"
    assert top.iloc[0]["anomaly_score"] == pytest.approx(0.9)


def test_filter_period_and_paths(tmp_path) -> None:
    frame = _silver()
    filtered = filter_period(frame, "2026-10-26", "2026-10-26")

    silver, gold, metadata = lake_paths(tmp_path, "yahoo")

    assert len(filtered) == 2
    assert silver.name == "table=quotes"
    assert gold.name == "table=anomalies"
    assert metadata.name == "_metadata.json"
