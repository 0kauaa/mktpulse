from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from mktpulse.features import FEATURE_COLUMNS, add_anomaly_features, prepare_features


def _feature_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["PETR4", "PETR4"],
            "trade_date": pd.to_datetime(["2026-01-02", "2026-01-05"]),
            "high": [11.0, 13.0],
            "low": [9.0, 8.0],
            "close": [10.0, 12.0],
            "adjusted_close": [10.0, 12.0],
            "volume": [1000, 3000],
            "daily_return": [0.10, 0.20],
            "log_return": [0.0953, 0.1823],
            "return_5d": [0.05, 0.15],
            "return_20d": [0.01, 0.30],
            "volatility_20d": [0.20, 0.25],
            "sma_20": [9.0, 10.0],
            "sma_50": [8.0, 9.0],
            "avg_volume_20d": [1000.0, 2000.0],
            "drawdown_60d": [-0.10, -0.05],
        }
    )


def test_add_anomaly_features_calculates_scale_free_features() -> None:
    result = add_anomaly_features(_feature_frame())

    assert result.loc[0, "sma_gap_20"] == pytest.approx(10.0 / 9.0 - 1.0)
    assert result.loc[1, "sma_gap_20"] == pytest.approx(12.0 / 10.0 - 1.0)
    assert result.loc[0, "volume_ratio_20d"] == pytest.approx(0.0)
    assert result.loc[1, "volume_ratio_20d"] == pytest.approx(0.5)
    assert result.loc[0, "range_pct"] == pytest.approx(0.2)
    assert result.loc[1, "close_location"] == pytest.approx(0.8)


def test_prepare_features_drops_incomplete_and_non_finite_rows() -> None:
    frame = _feature_frame()
    frame.loc[1, "volatility_20d"] = np.inf

    scored, matrix = prepare_features(frame)

    assert len(scored) == 1
    assert len(matrix) == 1
    assert list(matrix.columns) == FEATURE_COLUMNS
    assert np.isfinite(matrix.to_numpy()).all()
