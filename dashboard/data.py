"""Leitura e transformacoes usadas pelo dashboard."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


def lake_paths(lake_root: str | Path, source: str) -> tuple[Path, Path, Path]:
    root = Path(lake_root)
    silver = root / f"silver/source={source}/table=quotes"
    gold = root / f"gold/source={source}/table=anomalies"
    metadata = gold / "_metadata.json"
    return silver, gold, metadata


def load_silver(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    return frame.sort_values(["symbol", "trade_date"]).reset_index(drop=True)


def load_gold(path: Path) -> pd.DataFrame:
    if not path.exists() or next(path.rglob("*.parquet"), None) is None:
        return pd.DataFrame()
    frame = pd.read_parquet(path)
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    return frame.sort_values(["symbol", "trade_date"]).reset_index(drop=True)


def load_metadata(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def merge_anomalies(silver: pd.DataFrame, gold: pd.DataFrame) -> pd.DataFrame:
    result = silver.copy()
    if gold.empty:
        result["is_anomaly"] = False
        result["anomaly_score"] = float("nan")
        return result

    extra_columns = [
        column
        for column in gold.columns
        if column not in result.columns and column not in {"symbol", "trade_date"}
    ]
    result = result.merge(
        gold[["symbol", "trade_date", *extra_columns]],
        on=["symbol", "trade_date"],
        how="left",
    )
    result["is_anomaly"] = result["is_anomaly"].eq(True)
    return result


def summarize_symbols(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for symbol, group in frame.groupby("symbol", sort=True, observed=True):
        last = group.sort_values("trade_date").iloc[-1]
        rows.append(
            {
                "symbol": symbol,
                "ultimo_pregao": last["trade_date"].date(),
                "fechamento": last["adjusted_close"],
                "retorno_dia": last.get("daily_return"),
                "volatilidade_20d": last.get("volatility_20d"),
                "anomalias": int(group["is_anomaly"].sum()),
            }
        )
    return pd.DataFrame(rows)


def top_anomalies(frame: pd.DataFrame, limit: int = 20) -> pd.DataFrame:
    anomalies = frame[frame["is_anomaly"]].copy()
    if anomalies.empty:
        return anomalies
    columns = [
        column
        for column in [
            "trade_date",
            "symbol",
            "anomaly_score",
            "daily_return",
            "volatility_20d",
            "volume_ratio_20d",
            "close",
        ]
        if column in anomalies.columns
    ]
    return anomalies.sort_values("anomaly_score", ascending=False).head(limit)[columns]


def filter_period(
    frame: pd.DataFrame,
    start: Any,
    end: Any,
) -> pd.DataFrame:
    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)
    mask = (frame["trade_date"] >= start_ts) & (frame["trade_date"] <= end_ts)
    return frame.loc[mask]
