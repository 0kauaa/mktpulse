from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from mktpulse.lake import (
    build_envelope,
    date_from_epoch,
    decode_envelope,
    encode_envelope,
    raw_key,
)
from tests.conftest import epoch, make_result

def test_raw_key_preserves_arrival_date_and_symbol() -> None:
    key = raw_key("yahoo", "PETR4", "2026-11-17", "abc123")
    assert key == ("raw/source=yahoo/endpoint=quote/ingestion_date=2026-11-17/symbol=PETR4/part-abc123.json.gz")

def test_envelope_is_deterministic_and_roundtrips() -> None:
    envelope = build_envelope(source="brapi",
                              run_id="run-1",
                              symbol="PETR4",
                              request={"range": "1y", "interval": "1d"},
                              payload=make_result(),
                              ingested_at=datetime(2026, 11, 17, 18, 0, tzinfo=UTC))

    first = encode_envelope(envelope)
    second = encode_envelope(envelope)

    assert first == second
    assert decode_envelope(first) == envelope
    assert envelope["content_hash"] == envelope["content_hash"]
    assert len(envelope["content_hash"]) == 64


def test_content_hash_changes_when_payload_changes() -> None:
    args = {
        "source": "brapi",
        "run_id": "run-1",
        "symbol": "PETR4",
        "request": {"range": "1y"},
        "ingested_at": datetime(2026, 11, 17, 18, 0, tzinfo=UTC),
    }
    first = build_envelope(payload=make_result(), **args)
    second = build_envelope(payload=make_result(currency="USD"), **args)

    assert first["content_hash"] != second["content_hash"]


def test_content_hash_ignores_volatile_brapi_metadata() -> None:
    args = {
        "source": "brapi",
        "run_id": "run-1",
        "symbol": "PETR4",
        "request": {"range": "1y"},
        "ingested_at": datetime(2026, 11, 17, 18, 0, tzinfo=UTC),
    }
    first_payload = make_result()
    first_payload["regularMarketPrice"] = 10.0
    second_payload = make_result()
    second_payload["regularMarketPrice"] = 99.0

    first = build_envelope(payload=first_payload, **args)
    second = build_envelope(payload=second_payload, **args)

    assert first["content_hash"] == second["content_hash"]

def test_content_hash_tolerates_float_jitter() -> None:
    args = {
        "source": "brapi",
        "run_id": "run-1",
        "symbol": "PETR4",
        "request": {"range": "1y"},
        "ingested_at": datetime(2026, 11, 17, 18, 0, tzinfo=UTC),
    }
    first_payload = make_result()
    second_payload = make_result()
    second_payload["historicalDataPrice"][0]["adjustedClose"] += 0.000004

    assert (build_envelope(payload=first_payload, **args)["content_hash"]
        == build_envelope(payload=second_payload, **args)["content_hash"])

def test_content_hash_ignores_volatile_yahoo_metadata() -> None:
    base = {
        "chart": {
            "result": [
                {
                    "meta": {"symbol": "PETR4.SA", "currency": "BRL", "regularMarketPrice": 10.0},
                    "timestamp": [epoch(date(2026, 1, 2))],
                    "indicators": {
                        "quote": [
                            {
                                "open": [10.0],
                                "high": [11.0],
                                "low": [9.0],
                                "close": [10.5],
                                "volume": [1000],
                            }
                        ]
                    },
                }
            ]
        }
    }
    changed = {
        "chart": {
            **base["chart"],
            "result": [
                {
                    **base["chart"]["result"][0],
                    "meta": {
                        "symbol": "PETR4.SA",
                        "currency": "BRL",
                        "regularMarketPrice": 99.0,
                    },
                }
            ],
        }
    }
    args = {
        "source": "yahoo",
        "run_id": "run-1",
        "symbol": "PETR4",
        "request": {"range": "1y"},
        "ingested_at": datetime(2026, 11, 17, 18, 0, tzinfo=UTC),
    }

    assert (build_envelope(payload=base, **args)["content_hash"]
        == build_envelope(payload=changed, **args)["content_hash"])

def test_decode_rejects_corrupted_payload() -> None:
    with pytest.raises(ValueError):
        decode_envelope(b"not-gzip")

def test_epoch_is_converted_to_b3_calendar_date() -> None:
    assert date_from_epoch(epoch(date(2026, 1, 2))) == date(2026, 1, 2)

def test_epoch_in_milliseconds_is_accepted() -> None:
    assert date_from_epoch(epoch(date(2026, 1, 2)) * 1000) == date(2026, 1, 2)
