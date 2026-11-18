from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from mktpulse.lake import build_envelope, normalized_key
from mktpulse.normalize import normalize_envelope
from mktpulse.pipeline import IngestionPipeline, read_snapshot
from mktpulse.sources.yahoo import YahooClient, YahooError
from mktpulse.storage.local import LocalStorage
from tests.conftest import FIXED_NOW, epoch

def yahoo_payload(symbol: str = "PETR4.SA") -> dict[str, Any]:
    days = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]

    return {
        "chart": {
            "result": [
                {
                    "meta": {"symbol": symbol, "currency": "BRL"},
                    "timestamp": [epoch(day) + 10 * 3600 for day in days],
                    "indicators": {
                        "quote": [
                            {
                                "open": [10.0, 10.5, None, 12.0],
                                "high": [11.0, 11.5, None, 13.0],
                                "low": [9.0, 10.0, None, 11.5],
                                "close": [10.5, 11.0, None, 12.5],
                                "volume": [1000, 1100, None, 0],
                            }
                        ],
                        "adjclose": [{"adjclose": [10.2, 10.8, None, 12.1]}],
                    },
                }
            ],
            "error": None,
        }
    }

class FakeResponse:
    def __init__(self, status_code: int, payload: Any) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> Any:
        return self._payload

class FakeSession:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        return self.response

def test_yahoo_client_appends_sa_suffix_and_returns_mapping() -> None:
    session = FakeSession(FakeResponse(200, yahoo_payload()))
    client = YahooClient(session=session)

    quotes = client.fetch_quotes(["PETR4"], range_="1y", interval="1d")

    assert set(quotes) == {"PETR4"}
    call = session.calls[0]
    assert call["url"].endswith("/v8/finance/chart/PETR4.SA")
    assert call["params"]["range"] == "1y"
    assert call["params"]["includePrePost"] == "false"

def test_yahoo_client_maps_http_error() -> None:
    session = FakeSession(FakeResponse(404, {"chart": {"result": None, "error": {"description": "No data found"}}}))
    client = YahooClient(session=session)

    with pytest.raises(YahooError, match="No data found"):
        client.fetch_quotes(["XXXX"])

def test_yahoo_normalization_skips_incomplete_bars() -> None:
    envelope = build_envelope(source="yahoo",
                              run_id="run-1",
                              symbol="PETR4",
                              request={"range": "1y", "interval": "1d"},
                              payload=yahoo_payload(),
                              ingested_at=datetime(2026, 11, 17, 18, 0, tzinfo=UTC))

    frame = normalize_envelope(envelope)

    assert list(frame["symbol"]) == ["PETR4", "PETR4"]
    assert list(frame["trade_date"]) == [date(2026, 1, 2), date(2026, 1, 5)]
    assert frame.attrs["rows_received"] == 4
    assert frame.attrs["rows_skipped"] == 2
    assert frame.loc[0, "adjusted_close"] == 10.2
    assert frame.loc[0, "source"] == "yahoo"

class FakeYahooSource:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload

    def fetch_quotes(self, symbols: list[str], *, range_: str = "1y", interval: str = "1d") -> dict[str, dict[str, Any]]:
        return {symbol: self.payload for symbol in symbols}

def test_yahoo_pipeline_keeps_source_in_lake_paths(settings) -> None:
    yahoo_settings = settings.model_copy(update={"source": "yahoo"})
    storage = LocalStorage(yahoo_settings.lake_root)
    pipeline = IngestionPipeline(yahoo_settings, FakeYahooSource(yahoo_payload()), storage, now=lambda: FIXED_NOW)

    summary = pipeline.ingest(["PETR4"], run_id="run-1")

    assert summary.status == "success"
    assert summary.totals["rows_skipped"] == 2
    assert storage.list("raw/source=yahoo/")
    assert storage.exists(normalized_key("yahoo", "PETR4"))
    assert not storage.exists(normalized_key("brapi", "PETR4"))
    frame = read_snapshot(storage, "yahoo", "PETR4")
    assert len(frame) == 2
    assert summary.symbols["PETR4"].rows_received == 4
    assert summary.symbols["PETR4"].rows_skipped == 2
