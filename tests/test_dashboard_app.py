from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from streamlit.testing.v1 import AppTest


def test_dashboard_renders_with_silver_and_gold(tmp_path, monkeypatch) -> None:
    lake = tmp_path / "lake"
    silver_path = lake / "silver/source=yahoo/table=quotes"
    gold_path = lake / "gold/source=yahoo/table=anomalies"
    silver_path.mkdir(parents=True)
    gold_path.mkdir(parents=True)

    frame = pd.DataFrame(
        {
            "symbol": ["PETR4", "PETR4", "VALE3"],
            "trade_date": pd.to_datetime(["2026-10-24", "2026-10-26", "2026-10-26"]),
            "volume": [1000, 3000, 2000],
            "close": [30.0, 21.0, 60.0],
            "adjusted_close": [30.0, 21.0, 60.0],
            "sma_20": [29.0, 28.0, 59.0],
            "sma_50": [28.0, 27.0, 58.0],
            "daily_return": [0.0, -0.30, 0.01],
            "volatility_20d": [0.30, 0.42, 0.25],
            "volume_ratio_20d": [0.1, 0.8, 0.2],
            "anomaly_score": [0.1, 0.9, 0.2],
            "is_anomaly": [False, True, False],
        }
    )
    silver = frame.drop(columns=["anomaly_score", "is_anomaly"])
    silver.to_parquet(silver_path / "part.parquet", index=False)
    frame.to_parquet(gold_path / "part.parquet", index=False)
    (gold_path / "_metadata.json").write_text(
        json.dumps(
            {
                "run_id": "detect-test",
                "features": ["daily_return", "volatility_20d"],
                "rows_scored": 3,
                "anomalies": 1,
                "anomaly_rate": 0.333333,
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv("MKT_LAKE_ROOT", str(lake))
    monkeypatch.setenv("MKT_SOURCE", "yahoo")
    app = AppTest.from_file(
        Path(__file__).parents[1] / "dashboard" / "app.py",
        default_timeout=30,
    ).run()

    assert not app.exception
    assert app.title[0].value == "MarketPulse"
