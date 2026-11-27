"""deteccao de anomalias com isolation forest sobre a camada silver."""

from __future__ import annotations

import json
import logging
import shutil
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .config import Settings
from .features import FEATURE_COLUMNS, prepare_features
from .lake import (
    SCHEMA_VERSION,
    gold_prefix,
    ingestion_date,
    model_key,
    new_run_id,
    run_artifact_key,
    silver_prefix,
    utc_now,
)
from .observability import run_id_var
from .storage import Storage

logger = logging.getLogger("mktpulse.anomaly")

@dataclass
class AnomalySummary:
    run_id: str
    status: str
    started_at: str
    finished_at: str
    duration_seconds: float
    manifest_key: str
    metrics_key: str
    model_key: str
    gold_prefix: str
    totals: dict[str, Any]
    config: dict[str, Any]
    metrics: dict[str, Any] = field(default_factory=dict)
    operation: str = "detect"
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

def _normalize_symbols(symbols: Sequence[str] | str | None) -> list[str]:
    if symbols is None:
        return []
    raw = symbols.split(",") if isinstance(symbols, str) else list(symbols)
    seen: set[str] = set()
    normalized: list[str] = []
    for item in raw:
        ticker = item.strip().upper()
        if ticker and ticker not in seen:
            seen.add(ticker)
            normalized.append(ticker)
    return normalized

def _build_pipeline(
    *,
    contamination: float,
    n_estimators: int,
    random_state: int,
):
    from sklearn.ensemble import IsolationForest
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import RobustScaler

    return Pipeline(
        [
            ("scaler", RobustScaler()),
            (
                "model",
                IsolationForest(
                    n_estimators=n_estimators,
                    contamination=contamination,
                    random_state=random_state,
                    n_jobs=-1,
                ),
            ),
        ]
    )

def _safe_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    return float(value)

def _training_mask(
    frame: pd.DataFrame,
    *,
    train_fraction: float,
    min_training_rows: int,
) -> pd.Series:
    if train_fraction >= 1.0:
        return pd.Series(True, index=frame.index)

    dates = np.sort(frame["trade_date"].dt.normalize().unique())
    cutoff_index = max(0, int(len(dates) * train_fraction) - 1)
    cutoff = pd.Timestamp(dates[cutoff_index])
    mask = frame["trade_date"].dt.normalize() <= cutoff
    if int(mask.sum()) < min_training_rows:
        return pd.Series(True, index=frame.index)
    return mask

class AnomalyJob:
    def __init__(
        self,
        settings: Settings,
        storage: Storage,
        *,
        now: Any = utc_now,
    ) -> None:
        self.settings = settings
        self.source = settings.source
        self.storage = storage
        self._now = now

    def run(
        self,
        symbols: Sequence[str] | str | None = None,
        *,
        run_id: str | None = None,
        contamination: float | None = None,
        n_estimators: int | None = None,
        train_fraction: float | None = None,
    ) -> AnomalySummary:
        if self.settings.lake_backend != "local":
            raise RuntimeError("detect esta implementado apenas para lake local nesta semana")

        started_at = self._now()
        run_id = run_id or new_run_id(started_at)
        token = run_id_var.set(run_id)
        tickers = _normalize_symbols(symbols)
        contamination = contamination or self.settings.anomaly_contamination
        n_estimators = n_estimators or self.settings.anomaly_n_estimators
        train_fraction = (
            self.settings.anomaly_train_fraction if train_fraction is None else train_fraction
        )
        status = "failed"
        fatal_error: Exception | None = None
        error: str | None = None
        metrics: dict[str, Any] = {}
        rows_input = 0
        rows_scored = 0
        anomalies = 0
        symbols_scored = 0
        training_rows = 0

        logger.info(
            "detect iniciado",
            extra={
                "source": self.source,
                "symbols": tickers or "todos",
                "contamination": contamination,
            },
        )

        try:
            if not 0 < contamination < 0.5:
                raise ValueError("contamination deve estar entre 0 e 0.5")
            if n_estimators < 10:
                raise ValueError("n_estimators deve ser pelo menos 10")
            if not 0 < train_fraction <= 1:
                raise ValueError("train_fraction deve estar entre 0 e 1")

            silver_root = self._silver_root()
            frame = self._read_silver(silver_root, tickers)
            rows_input = len(frame)

            scored, matrix = prepare_features(frame)
            rows_scored = len(scored)
            if scored.empty:
                raise ValueError("nenhuma linha com features completas; rode transform antes")

            mask = _training_mask(
                scored,
                train_fraction=train_fraction,
                min_training_rows=self.settings.anomaly_min_training_rows,
            )
            training_rows = int(mask.sum())
            pipeline = _build_pipeline(
                contamination=contamination,
                n_estimators=n_estimators,
                random_state=self.settings.anomaly_random_state,
            )
            pipeline.fit(matrix.loc[mask])
            scores = -pipeline.score_samples(matrix)
            predictions = pipeline.predict(matrix) == -1

            scored_at = self._now()
            scored = scored.copy()
            scored["anomaly_score"] = scores
            scored["is_anomaly"] = predictions
            scored["year"] = scored["trade_date"].dt.year
            scored["month"] = scored["trade_date"].dt.month
            scored["model_run_id"] = run_id
            scored["scored_at"] = scored_at

            self._write_model(run_id, pipeline)
            gold_key = self._write_gold(scored, run_id)
            metrics = self._build_metrics(
                scored=scored,
                mask=mask,
                rows_input=rows_input,
                run_id=run_id,
                generated_at=scored_at,
                contamination=contamination,
                n_estimators=n_estimators,
                train_fraction=train_fraction,
                gold_key=gold_key,
            )
            anomalies = int(metrics["anomalies"])
            symbols_scored = int(metrics["symbols"])
            self._write_metrics(run_id, started_at, metrics)
            self._write_metadata(run_id, gold_key, metrics)
            status = "success"
            logger.info(
                "anomalias detectadas",
                extra={
                    "rows_scored": rows_scored,
                    "anomalies": anomalies,
                    "anomaly_rate": metrics["anomaly_rate"],
                    "gold_key": gold_key,
                },
            )
        except Exception as exc:  # noqa: ble001 - garante manifest e status failure
            fatal_error = exc
            status = "failed"
            error = str(exc)
            logger.exception("detect abortado")
        finally:
            finished_at = self._now()
            summary = self._build_summary(
                run_id=run_id,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                rows_input=rows_input,
                rows_scored=rows_scored,
                anomalies=anomalies,
                symbols=symbols_scored,
                training_rows=training_rows,
                contamination=contamination,
                n_estimators=n_estimators,
                train_fraction=train_fraction,
                metrics=metrics,
                error=error,
            )
            self._write_manifest(summary)
            run_id_var.reset(token)

        if fatal_error:
            raise fatal_error
        return summary

    def _silver_root(self) -> Path:
        root = (self.settings.lake_root / silver_prefix(self.source)).resolve()
        if not root.exists() or next(root.rglob("*.parquet"), None) is None:
            raise FileNotFoundError(
                f"nenhum Parquet silver encontrado em {root}; rode transform primeiro"
            )
        return root

    def _read_silver(self, root: Path, tickers: Sequence[str]) -> pd.DataFrame:
        frame = pd.read_parquet(root)
        if tickers:
            frame = frame[frame["symbol"].isin(tickers)]
        if frame.empty:
            raise FileNotFoundError(f"nenhuma linha silver encontrada para symbols={list(tickers)}")
        return frame

    def _write_model(self, run_id: str, pipeline: Any) -> None:
        import joblib

        path = (self.settings.lake_root / model_key(run_id)).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.tmp-{run_id}")
        joblib.dump(pipeline, temporary)
        temporary.replace(path)

    def _write_gold(self, frame: pd.DataFrame, run_id: str) -> str:
        relative = gold_prefix(self.source)
        output = (self.settings.lake_root / relative).resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.parent / f".{output.name}.tmp-{run_id}"
        if temporary.exists():
            shutil.rmtree(temporary)
        temporary.mkdir(parents=True)
        frame.to_parquet(
            temporary,
            partition_cols=["symbol", "year", "month"],
            index=False,
        )
        if output.exists():
            shutil.rmtree(output)
        temporary.replace(output)
        return relative

    def _build_metrics(
        self,
        *,
        scored: pd.DataFrame,
        mask: pd.Series,
        rows_input: int,
        run_id: str,
        generated_at: datetime,
        contamination: float,
        n_estimators: int,
        train_fraction: float,
        gold_key: str,
    ) -> dict[str, Any]:
        anomalies = int(scored["is_anomaly"].sum())
        holdout = ~mask
        holdout_rows = int(holdout.sum())
        holdout_anomalies = int(scored.loc[holdout, "is_anomaly"].sum()) if holdout_rows else 0
        training = scored.loc[mask]
        scores = scored["anomaly_score"]

        quantiles = {
            f"p{percentile}": float(np.percentile(scores, percentile))
            for percentile in (50, 90, 95, 99)
        }
        top = scored.sort_values("anomaly_score", ascending=False).head(20)
        top_anomalies = [
            {
                "symbol": row.symbol,
                "trade_date": row.trade_date.date().isoformat(),
                "anomaly_score": float(row.anomaly_score),
                "is_anomaly": bool(row.is_anomaly),
                "daily_return": _safe_float(row.daily_return),
                "volatility_20d": _safe_float(row.volatility_20d),
                "volume_ratio_20d": _safe_float(row.volume_ratio_20d),
                "close": _safe_float(row.close),
            }
            for row in top.itertuples()
        ]

        return {
            "run_id": run_id,
            "generated_at": generated_at.astimezone(UTC).isoformat(),
            "source": self.source,
            "rows_input": rows_input,
            "rows_scored": len(scored),
            "rows_dropped": rows_input - len(scored),
            "symbols": int(scored["symbol"].nunique()),
            "anomalies": anomalies,
            "anomaly_rate": round(anomalies / len(scored), 6),
            "training_rows": len(training),
            "training_start": training["trade_date"].min().date().isoformat(),
            "training_end": training["trade_date"].max().date().isoformat(),
            "holdout_rows": holdout_rows,
            "holdout_anomalies": holdout_anomalies,
            "holdout_anomaly_rate": (
                round(holdout_anomalies / holdout_rows, 6) if holdout_rows else 0.0
            ),
            "feature_columns": FEATURE_COLUMNS,
            "params": {
                "contamination": contamination,
                "n_estimators": n_estimators,
                "random_state": self.settings.anomaly_random_state,
                "train_fraction": train_fraction,
                "scaler": "RobustScaler",
                "model": "IsolationForest",
            },
            "score_quantiles": quantiles,
            "gold_key": gold_key,
            "model_key": model_key(run_id),
            "top_anomalies": top_anomalies,
        }

    def _write_metrics(
        self,
        run_id: str,
        started_at: datetime,
        metrics: dict[str, Any],
    ) -> None:
        key = run_artifact_key(
            run_id,
            ingestion_date(started_at),
            "anomaly_metrics.json",
        )
        body = json.dumps(metrics, indent=2, ensure_ascii=False, default=str).encode("utf-8")
        self.storage.write_bytes(key, body)

    def _write_metadata(
        self,
        run_id: str,
        gold_key: str,
        metrics: dict[str, Any],
    ) -> None:
        metadata = {
            "run_id": run_id,
            "generated_at": metrics["generated_at"],
            "model_key": model_key(run_id),
            "gold_key": gold_key,
            "features": metrics["feature_columns"],
            "params": metrics["params"],
            "rows_scored": metrics["rows_scored"],
            "anomalies": metrics["anomalies"],
            "anomaly_rate": metrics["anomaly_rate"],
        }
        body = json.dumps(metadata, indent=2, ensure_ascii=False, default=str).encode("utf-8")
        self.storage.write_bytes(f"{gold_key}_metadata.json", body)

    def _build_summary(
        self,
        *,
        run_id: str,
        status: str,
        started_at: datetime,
        finished_at: datetime,
        rows_input: int,
        rows_scored: int,
        anomalies: int,
        symbols: int,
        training_rows: int,
        contamination: float,
        n_estimators: int,
        train_fraction: float,
        metrics: dict[str, Any],
        error: str | None,
    ) -> AnomalySummary:
        return AnomalySummary(
            run_id=run_id,
            status=status,
            started_at=started_at.astimezone(UTC).isoformat(),
            finished_at=finished_at.astimezone(UTC).isoformat(),
            duration_seconds=round((finished_at - started_at).total_seconds(), 3),
            manifest_key=run_artifact_key(
                run_id,
                ingestion_date(started_at),
                "anomaly_manifest.json",
            ),
            metrics_key=run_artifact_key(
                run_id,
                ingestion_date(started_at),
                "anomaly_metrics.json",
            ),
            model_key=model_key(run_id),
            gold_prefix=gold_prefix(self.source),
            totals={
                "rows_input": rows_input,
                "rows_scored": rows_scored,
                "anomalies": anomalies,
                "anomaly_rate": (round(anomalies / rows_scored, 6) if rows_scored else 0.0),
                "symbols": symbols,
                "training_rows": training_rows,
            },
            config={
                "source": self.source,
                "input": silver_prefix(self.source),
                "output": gold_prefix(self.source),
                "contamination": contamination,
                "n_estimators": n_estimators,
                "train_fraction": train_fraction,
                "random_state": self.settings.anomaly_random_state,
            },
            metrics=metrics,
            error=error,
        )

    def _write_manifest(self, summary: AnomalySummary) -> None:
        manifest = {
            "schema_version": SCHEMA_VERSION,
            **summary.to_dict(),
        }
        body = json.dumps(manifest, indent=2, ensure_ascii=False, default=str).encode("utf-8")
        try:
            self.storage.write_bytes(summary.manifest_key, body)
        except Exception:  # noqa: ble001 - manifest nao pode mascarar o resultado
            logger.exception(
                "falha ao gravar manifest de detect",
                extra={"manifest_key": summary.manifest_key},
            )
