from __future__ import annotations

import json

import pandas as pd
import pytest

from mktpulse.anomaly import AnomalyJob
from mktpulse.features import FEATURE_COLUMNS
from mktpulse.lake import gold_prefix, model_key
from mktpulse.storage.local import LocalStorage
from tests.conftest import FIXED_NOW, make_silver_frame, write_silver


def _job(settings) -> tuple[AnomalyJob, LocalStorage]:
    storage = LocalStorage(settings.lake_root)
    return AnomalyJob(settings, storage, now=lambda: FIXED_NOW), storage


def test_detect_writes_gold_model_and_metrics(settings) -> None:
    frame = pd.concat(
        [
            make_silver_frame("PETR4", anomaly=True, seed=1),
            make_silver_frame("VALE3", anomaly=False, seed=2),
        ],
        ignore_index=True,
    )
    write_silver(settings.lake_root, "brapi", frame)
    job, storage = _job(settings)

    summary = job.run(
        run_id="detect-1",
        contamination=0.05,
        n_estimators=50,
        train_fraction=1.0,
    )

    gold = pd.read_parquet(settings.lake_root / gold_prefix("brapi"))
    metrics = json.loads(storage.read_bytes(summary.metrics_key))
    metadata = json.loads(storage.read_bytes(f"{gold_prefix('brapi')}_metadata.json"))
    model_path = settings.lake_root / model_key("detect-1")

    assert summary.status == "success"
    assert summary.totals["rows_input"] == 320
    assert summary.totals["rows_scored"] > 0
    assert summary.totals["anomalies"] > 0
    assert gold["is_anomaly"].any()
    assert {"anomaly_score", "is_anomaly", "model_run_id"}.issubset(gold.columns)
    assert model_path.exists()
    assert metrics["feature_columns"] == FEATURE_COLUMNS
    assert metrics["params"]["model"] == "IsolationForest"
    assert metrics["top_anomalies"]
    assert metrics["anomaly_rate"] == pytest.approx(
        metrics["anomalies"] / metrics["rows_scored"], abs=1e-6
    )
    assert metadata["run_id"] == "detect-1"
    assert metadata["model_key"] == model_key("detect-1")


def test_detect_uses_time_based_holdout_when_enough_rows(settings) -> None:
    tuned = settings.model_copy(update={"anomaly_min_training_rows": 50})
    frame = pd.concat(
        [
            make_silver_frame("PETR4", anomaly=True, seed=3),
            make_silver_frame("VALE3", anomaly=False, seed=4),
        ],
        ignore_index=True,
    )
    write_silver(tuned.lake_root, "brapi", frame)
    job, storage = _job(tuned)

    summary = job.run(
        run_id="detect-holdout",
        contamination=0.05,
        n_estimators=50,
        train_fraction=0.7,
    )
    metrics = json.loads(storage.read_bytes(summary.metrics_key))

    assert summary.status == "success"
    assert metrics["holdout_rows"] > 0
    assert metrics["training_rows"] + metrics["holdout_rows"] == metrics["rows_scored"]
    assert metrics["holdout_anomaly_rate"] >= 0
