"""Features para deteccao de anomalias no mercado.

As features usam apenas informacao disponivel ate o pregao corrente: retornos,
volatilidade passada, distancia de medias e comportamento de volume. Preco
absoluto nao entra no modelo para nao comparar PETR4 com MGLU3 em escalas
diferentes.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

FEATURE_COLUMNS = [
    "daily_return",
    "log_return",
    "return_5d",
    "return_20d",
    "volatility_20d",
    "drawdown_60d",
    "sma_gap_20",
    "sma_gap_50",
    "volume_ratio_20d",
    "range_pct",
    "close_location",
]

REQUIRED_COLUMNS = {
    "symbol",
    "trade_date",
    "high",
    "low",
    "close",
    "adjusted_close",
    "volume",
    "daily_return",
    "log_return",
    "return_5d",
    "return_20d",
    "volatility_20d",
    "sma_20",
    "sma_50",
    "avg_volume_20d",
    "drawdown_60d",
}


def add_anomaly_features(frame: pd.DataFrame) -> pd.DataFrame:
    missing = REQUIRED_COLUMNS.difference(frame.columns)
    if missing:
        raise ValueError(f"silver sem colunas obrigatorias: {sorted(missing)}")

    result = frame.copy()
    result["trade_date"] = pd.to_datetime(result["trade_date"])
    result = result.sort_values(["symbol", "trade_date"], kind="stable").reset_index(drop=True)

    close = pd.to_numeric(result["adjusted_close"], errors="coerce")
    sma_20 = pd.to_numeric(result["sma_20"], errors="coerce")
    sma_50 = pd.to_numeric(result["sma_50"], errors="coerce")
    average_volume = pd.to_numeric(result["avg_volume_20d"], errors="coerce")
    volume = pd.to_numeric(result["volume"], errors="coerce")
    high = pd.to_numeric(result["high"], errors="coerce")
    low = pd.to_numeric(result["low"], errors="coerce")
    raw_close = pd.to_numeric(result["close"], errors="coerce")

    result["sma_gap_20"] = close / sma_20.where(sma_20 > 0) - 1.0
    result["sma_gap_50"] = close / sma_50.where(sma_50 > 0) - 1.0
    result["volume_ratio_20d"] = volume / average_volume.where(average_volume > 0) - 1.0

    high_low = high - low
    result["range_pct"] = high_low / raw_close.where(raw_close > 0)
    result["close_location"] = ((raw_close - low) / high_low.where(high_low > 0)).fillna(0.5)

    return result


def prepare_features(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    enriched = add_anomaly_features(frame)
    matrix = enriched[FEATURE_COLUMNS].replace([np.inf, -np.inf], np.nan)
    valid = matrix.notna().all(axis=1)
    scored = enriched.loc[valid].reset_index(drop=True)
    return scored, matrix.loc[valid].astype("float64").reset_index(drop=True)
