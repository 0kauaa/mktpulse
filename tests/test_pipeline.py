from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

import pandas as pd

from mktpulse.lake import normalized_key
from mktpulse.pipeline import IngestionPipeline, SymbolOutcome, read_snapshot
from mktpulse.sources.brapi import BrapiError
from mktpulse.storage.local import LocalStorage
from tests.conftest import FIXED_NOW, make_result, quote_row


class FakeBrapiClient:
    def __init__(self, results: dict[str, dict[str, Any]], failing: set[str] | None = None) -> None:
        self.results = results
        self.failing = failing or set()
        self.calls: list[list[str]] = []

    def fetch_quotes(
        self, symbols: list[str], *, range_: str = "1y", interval: str = "1d"
    ) -> dict[str, Any]:
        self.calls.append(list(symbols))
        if self.failing.intersection(symbols):
            raise BrapiError("falha simulada de rede")
        return {symbol: self.results[symbol] for symbol in symbols}


def build_pipeline(
    settings,
    results: dict[str, dict[str, Any]],
    *,
    failing: set[str] | None = None,
    now: datetime = FIXED_NOW,
) -> tuple[IngestionPipeline, LocalStorage, FakeBrapiClient]:

    storage = LocalStorage(settings.lake_root)
    client = FakeBrapiClient(results, failing)
    pipeline = IngestionPipeline(settings, client, storage, now=lambda: now)
    return pipeline, storage, client


def raw_keys(storage: LocalStorage) -> list[str]:
    return storage.list("raw/")


def test_ingest_writes_raw_and_normalized(settings) -> None:
    results = {"PETR4": make_result("PETR4"), "VALE3": make_result("VALE3")}
    pipeline, storage, _ = build_pipeline(settings, results)

    summary = pipeline.ingest(["PETR4", "VALE3"], run_id="run-1")

    assert summary.status == "success"
    assert summary.totals["raw_objects_written"] == 2
    assert summary.totals["rows_received"] == 4
    assert summary.totals["rows_stored"] == 4
    assert len(raw_keys(storage)) == 2
    assert len(read_snapshot(storage, "brapi", "PETR4")) == 2
    assert len(read_snapshot(storage, "brapi", "VALE3")) == 2


def test_rerun_is_idempotent_at_the_raw_layer(settings) -> None:
    results = {"PETR4": make_result("PETR4"), "VALE3": make_result("VALE3")}
    pipeline, storage, _ = build_pipeline(settings, results)

    pipeline.ingest(["PETR4", "VALE3"], run_id="run-1")
    keys_before = raw_keys(storage)

    second = pipeline.ingest(["PETR4", "VALE3"], run_id="run-2")

    assert second.status == "success"
    assert second.totals["raw_objects_written"] == 0
    assert second.totals["raw_objects_skipped"] == 2
    assert raw_keys(storage) == keys_before
    assert len(read_snapshot(storage, "brapi", "PETR4")) == 2


def test_late_row_is_merged_by_business_key(settings) -> None:
    results = {"PETR4": make_result("PETR4")}
    pipeline, storage, _ = build_pipeline(settings, results)
    pipeline.ingest(["PETR4"], run_id="run-1")

    results["PETR4"]["historicalDataPrice"].append(quote_row(date(2026, 1, 6), close=11.0))
    summary = pipeline.ingest(["PETR4"], run_id="run-2")

    assert summary.status == "success"
    assert summary.totals["raw_objects_written"] == 1
    assert len(raw_keys(storage)) == 2
    frame = read_snapshot(storage, "brapi", "PETR4")
    assert len(frame) == 3
    assert frame["trade_date"].max() == date(2026, 1, 6)


def test_failure_is_isolated_per_symbol(settings) -> None:
    isolated = settings.model_copy(update={"brapi_batch_size": 1})
    results = {
        "PETR4": make_result("PETR4"),
        "BAD": make_result("BAD"),
        "VALE3": make_result("VALE3"),
    }
    pipeline, storage, _ = build_pipeline(isolated, results, failing={"BAD"})

    summary = pipeline.ingest(["PETR4", "BAD", "VALE3"], run_id="run-1")

    assert summary.status == "partial"
    assert summary.totals["symbols_ok"] == 2
    assert summary.totals["symbols_failed"] == 1
    assert summary.symbols["BAD"].error == "falha simulada de rede"
    assert storage.exists(normalized_key("brapi", "PETR4"))
    assert storage.exists(normalized_key("brapi", "VALE3"))
    assert not storage.exists(normalized_key("brapi", "BAD"))

    manifest = json.loads(storage.read_bytes(summary.manifest_key))
    assert manifest["status"] == "partial"
    assert manifest["operation"] == "ingest"
    assert manifest["symbols"]["BAD"]["status"] == "error"


def test_raw_is_preserved_when_normalization_fails(settings) -> None:
    bad_row = quote_row(date(2026, 1, 2), high=11.0, close=12.0)
    results = {"PETR4": make_result("PETR4", rows=[bad_row])}
    pipeline, storage, _ = build_pipeline(settings, results)

    summary = pipeline.ingest(["PETR4"], run_id="run-1")

    assert summary.status == "failed"
    outcome = summary.symbols["PETR4"]
    assert outcome.status == "error"
    assert outcome.raw_written is True
    assert outcome.raw_key is not None
    assert storage.exists(outcome.raw_key)
    assert not storage.exists(normalized_key("brapi", "PETR4"))
    assert "OHLC inconsistente" in (outcome.error or "")


def test_rebuild_recreates_normalized_from_raw(settings) -> None:
    results = {"PETR4": make_result("PETR4")}
    pipeline, storage, _ = build_pipeline(settings, results)
    pipeline.ingest(["PETR4"], run_id="run-1")
    before = read_snapshot(storage, "brapi", "PETR4")

    (settings.lake_root / normalized_key("brapi", "PETR4")).unlink()
    summary = pipeline.rebuild(["PETR4"], run_id="rebuild-1")

    assert summary.status == "success"
    assert summary.operation == "rebuild"
    after = read_snapshot(storage, "brapi", "PETR4")
    pd.testing.assert_frame_equal(before, after)
    manifest = json.loads(storage.read_bytes(summary.manifest_key))
    assert manifest["operation"] == "rebuild"


def test_rebuild_discovers_symbols(settings) -> None:
    results = {"PETR4": make_result("PETR4"), "VALE3": make_result("VALE3")}
    pipeline, _, _ = build_pipeline(settings, results)
    pipeline.ingest(["PETR4", "VALE3"], run_id="run-1")

    summary = pipeline.rebuild(run_id="rebuild-1")

    assert summary.status == "success"
    assert set(summary.symbols) == {"PETR4", "VALE3"}


def test_corrupt_raw_does_not_overwrite_snapshot(settings) -> None:
    results = {"PETR4": make_result("PETR4")}
    pipeline, storage, _ = build_pipeline(settings, results)
    pipeline.ingest(["PETR4"], run_id="run-1")
    before = read_snapshot(storage, "brapi", "PETR4")

    corrupt_key = (
        "raw/source=brapi/endpoint=quote/ingestion_date=2026-11-17/"
        "symbol=PETR4/part-corrupt.json.gz"
    )
    storage.write_bytes(corrupt_key, b"not-a-valid-envelope")

    summary = pipeline.rebuild(["PETR4"], run_id="rebuild-2")

    assert summary.status == "failed"
    outcome: SymbolOutcome = summary.symbols["PETR4"]
    assert outcome.status == "error"
    assert "snapshot" in (outcome.error or "")
    after = read_snapshot(storage, "brapi", "PETR4")
    pd.testing.assert_frame_equal(before, after)
