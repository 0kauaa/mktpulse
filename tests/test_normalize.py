from __future__ import annotations

from datetime import UTC, date, datetime

import pytest
from mktpulse.lake import build_envelope
from mktpulse.normalize import QUOTES_COLUMNS, DataQualityError, normalize_envelope
from tests.conftest import make_result, quote_row

def _frame(result=None):
    envelope = build_envelope(
        source="brapi",
        run_id="run-1",
        symbol="PETR4",
        request={"range": "1y", "interval": "1d"},
        payload=result or make_result(),
        ingested_at=datetime(2026, 11, 17, 18, 0, tzinfo=UTC),
    )

    return normalize_envelope(envelope)

def test_valid_payload_is_normalized_and_sorted() -> None:
    result = make_result(rows=[quote_row(date(2026, 1, 5)), quote_row(date(2026, 1, 2))])

    frame = _frame(result)

    assert list(frame.columns) == QUOTES_COLUMNS
    assert frame["symbol"].tolist() == ["PETR4", "PETR4"]
    assert frame["trade_date"].tolist() == [date(2026, 1, 2), date(2026, 1, 5)]
    assert frame["volume"].dtype == "int64"
    assert str(frame["ingested_at"].dtype) == "datetime64[ns, UTC]"

def test_duplicate_trade_date_keeps_latest_row() -> None:
    result = make_result(
        rows=[
            quote_row(date(2026, 1, 2), close=10.0),
            quote_row(date(2026, 1, 2), close=12.0, high=13.0),
        ]
    )

    frame = _frame(result)

    assert len(frame) == 1
    assert frame.loc[0, "close"] == 12.0

def test_missing_adjusted_close_falls_back_to_close() -> None:
    row = quote_row(date(2026, 1, 2))
    row.pop("adjustedClose")

    frame = _frame(make_result(rows=[row]))

    assert frame.loc[0, "adjusted_close"] == frame.loc[0, "close"]

def test_inconsistent_ohlc_is_rejected() -> None:
    row = quote_row(date(2026, 1, 2), high=11.0, close=12.0)

    with pytest.raises(DataQualityError, match="OHLC inconsistente"):
        _frame(make_result(rows=[row]))

def test_empty_history_is_rejected() -> None:
    with pytest.raises(DataQualityError, match="historicalDataPrice vazio"):
        _frame(make_result(rows=[]))
