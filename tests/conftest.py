from __future__ import annotations

from datetime import UTC, date, datetime, time
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from mktpulse.config import Settings

B3_TZ = ZoneInfo("America/Sao_Paulo")
FIXED_NOW = datetime(2026, 11, 17, 15, 30, tzinfo=UTC)


def epoch(day: date) -> int:
    return int(datetime.combine(day, time(0), tzinfo=B3_TZ).timestamp())

def quote_row(day: date, *, open_: float = 10.0, high: float = 11.0, low: float = 9.0,
              close: float = 10.5, volume: int = 1000, adjusted: float | None = None) -> dict[str, Any]:

    return {
        "date": epoch(day),
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "adjustedClose": close if adjusted is None else adjusted,
    }

def make_result(symbol: str = "PETR4", rows: list[dict[str, Any]] | None = None, *, currency: str = "BRL") -> dict[str, Any]:
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
    return Settings(_env_file=None,
                    source="brapi",
                    lake_backend="local",
                    lake_root=tmp_path / "lake",
                    brapi_batch_size=3,
                    request_pause_seconds=0,
                    symbols="PETR4,VALE3")
