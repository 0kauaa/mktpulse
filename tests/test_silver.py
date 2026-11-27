from __future__ import annotations

import json
from datetime import UTC, datetime

import pandas as pd

from mktpulse.lake import normalized_key, silver_prefix
from mktpulse.silver import SilverJob
from mktpulse.storage.local import LocalStorage
from tests.conftest import make_quotes_frame, write_normalized

FIXED_NOW = datetime(2026, 1, 20, 12, 0, tzinfo=UTC)

def test_transform_writes_partitioned_silver_and_is_idempotent(spark, settings) -> None:
    frame = pd.concat(
        [make_quotes_frame("PETR4", days=10), make_quotes_frame("VALE3", days=10)],
        ignore_index=True,
    )
    write_normalized(settings.lake_root, "brapi", frame)

    storage = LocalStorage(settings.lake_root)
    job = SilverJob(settings, storage, spark=spark, now=lambda: FIXED_NOW)
    first = job.run(run_id="transform-1")
    root = settings.lake_root / silver_prefix("brapi")
    silver = spark.read.parquet(str(root))

    assert first.status == "success"
    assert first.totals["rows_read"] == 20
    assert first.totals["rows_written"] == 20
    assert first.totals["partitions_written"] == 2
    assert silver.count() == 20
    assert silver.select("symbol", "trade_date").distinct().count() == 20
    assert "daily_return" in silver.columns

    second = job.run(run_id="transform-2")

    assert second.status == "success"
    assert second.totals["rows_written"] == 20
    assert spark.read.parquet(str(root)).count() == 20
    assert spark.read.parquet(str(root)).select("symbol", "trade_date").distinct().count() == 20

    manifest = json.loads(storage.read_bytes(first.manifest_key))
    report = json.loads(storage.read_bytes(first.quality_report_key))

    assert manifest["operation"] == "transform"
    assert manifest["quality"]["status"] == "success"
    assert report["total_rows"] == 20
    assert report["status"] == "success"

def test_critical_quality_failure_does_not_write_silver(spark, settings) -> None:
    frame = make_quotes_frame("PETR4", days=5, bad=True)
    write_normalized(settings.lake_root, "brapi", frame)
    storage = LocalStorage(settings.lake_root)
    job = SilverJob(settings, storage, spark=spark, now=lambda: FIXED_NOW)

    summary = job.run(run_id="transform-bad")
    root = settings.lake_root / silver_prefix("brapi")
    manifest = json.loads(storage.read_bytes(summary.manifest_key))
    report = json.loads(storage.read_bytes(summary.quality_report_key))

    assert summary.status == "failed"
    assert summary.totals["rows_written"] == 0
    assert not root.exists()
    assert manifest["status"] == "failed"
    assert manifest["quality"]["critical_failures"] >= 1
    assert report["status"] == "failed"

def test_full_refresh_removes_stale_silver_partition(spark, settings) -> None:
    frame = pd.concat(
        [
            make_quotes_frame("PETR4", days=10),
            make_quotes_frame("VALE3", days=10),
        ],
        ignore_index=True,
    )
    write_normalized(settings.lake_root, "brapi", frame)

    storage = LocalStorage(settings.lake_root)
    job = SilverJob(settings, storage, spark=spark, now=lambda: FIXED_NOW)
    job.run(run_id="transform-1")
    root = settings.lake_root / silver_prefix("brapi")

    (settings.lake_root / normalized_key("brapi", "VALE3")).unlink()
    summary = job.run(run_id="transform-refresh", full_refresh=True)
    silver = spark.read.parquet(str(root))

    assert summary.status == "success"
    assert summary.totals["rows_written"] == 10
    assert silver.count() == 10
    assert silver.select("symbol").distinct().collect()[0].symbol == "PETR4"
