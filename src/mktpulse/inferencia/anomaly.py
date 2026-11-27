"""
deteccao de anomalias com isolation forest sobre a camada silver.
"""

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

from mktpulse.comum import config as comum_config
from mktpulse.comum import observability as comum_observability
from mktpulse.comum import symbols as comum_symbols
from mktpulse.inferencia import features as inferencia_features
from mktpulse.processamento import (
    artifacts as processamento_artifacts,
    lake as processamento_lake,
    storage as processamento_storage,
)

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


@dataclass(frozen=True)
class _ModelParams:
    contamination: float
    n_estimators: int
    train_fraction: float


@dataclass
class _RunResult:
    status: str = "failed"
    metrics: dict[str, Any] = field(default_factory=dict)
    rows_input: int = 0
    rows_scored: int = 0
    anomalies: int = 0
    symbols_scored: int = 0
    training_rows: int = 0
    error: str | None = None


def _build_pipeline(
    *,
    contamination: float,
    n_estimators: int,
    random_state: int,
) -> Any:
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


def _score_frame(
    frame: pd.DataFrame,
    matrix: pd.DataFrame,
    pipeline: Any,
    *,
    run_id: str,
    scored_at: datetime,
) -> pd.DataFrame:
    scored = frame.copy()
    scored["anomaly_score"] = -pipeline.score_samples(matrix)
    scored["is_anomaly"] = pipeline.predict(matrix) == -1
    scored["year"] = scored["trade_date"].dt.year
    scored["month"] = scored["trade_date"].dt.month
    scored["model_run_id"] = run_id
    scored["scored_at"] = scored_at
    return scored


def _top_anomalies(frame: pd.DataFrame) -> list[dict[str, Any]]:
    top = frame.sort_values("anomaly_score", ascending=False).head(20)
    return [
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


class AnomalyJob:
    def __init__(
        self,
        settings: comum_config.Settings,
        storage: processamento_storage.Storage,
        *,
        now: Any = processamento_lake.utc_now,
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
        run_id = run_id or processamento_lake.new_run_id(started_at)
        token = comum_observability.run_id_var.set(run_id)
        tickers = comum_symbols.normalize_symbols(symbols)
        params = self._resolve_params(contamination, n_estimators, train_fraction)
        result = _RunResult()
        fatal_error: Exception | None = None

        with comum_observability.run_context(run_id):
            logger.info(
                "detect iniciado",
                extra={
                    "source": self.source,
                    "symbols": tickers or "todos",
                    "contamination": params.contamination,
                },
            )
            try:
                result = self._execute(run_id, started_at, tickers, params)
            except Exception as exc:  # noqa: ble001 - garante manifest e status failure
                fatal_error = exc
                result.error = str(exc)
                logger.exception("detect abortado")

            summary = self._build_summary(
                run_id=run_id,
                started_at=started_at,
                finished_at=self._now(),
                result=result,
                params=params,
            )
            self._write_manifest(summary)

        if fatal_error:
            raise fatal_error
        return summary

    def _resolve_params(
        self,
        contamination: float | None,
        n_estimators: int | None,
        train_fraction: float | None,
    ) -> _ModelParams:
        return _ModelParams(
            contamination=contamination or self.settings.anomaly_contamination,
            n_estimators=n_estimators or self.settings.anomaly_n_estimators,
            train_fraction=(
                self.settings.anomaly_train_fraction if train_fraction is None else train_fraction
            ),
        )

    def _execute(
        self,
        run_id: str,
        started_at: datetime,
        tickers: Sequence[str],
        params: _ModelParams,
    ) -> _RunResult:
        _validate_params(params)
        result = _RunResult()

        frame = self._read_silver(self._silver_root(), tickers)
        result.rows_input = len(frame)

        scored, matrix = inferencia_features.prepare_features(frame)
        result.rows_scored = len(scored)
        if scored.empty:
            raise ValueError("nenhuma linha com features completas; rode transform antes")

        mask = _training_mask(
            scored,
            train_fraction=params.train_fraction,
            min_training_rows=self.settings.anomaly_min_training_rows,
        )
        result.training_rows = int(mask.sum())
        pipeline = _build_pipeline(
            contamination=params.contamination,
            n_estimators=params.n_estimators,
            random_state=self.settings.anomaly_random_state,
        )
        pipeline.fit(matrix.loc[mask])

        scored_at = self._now()
        scored = _score_frame(scored, matrix, pipeline, run_id=run_id, scored_at=scored_at)
        self._write_model(run_id, pipeline)
        gold_key = self._write_gold(scored, run_id)
        result.metrics = self._build_metrics(
            scored=scored,
            mask=mask,
            rows_input=result.rows_input,
            run_id=run_id,
            generated_at=scored_at,
            params=params,
            gold_key=gold_key,
        )
        result.anomalies = int(result.metrics["anomalies"])
        result.symbols_scored = int(result.metrics["symbols"])
        self._write_metrics(run_id, started_at, result.metrics)
        self._write_metadata(run_id, gold_key, result.metrics)
        result.status = "success"
        logger.info(
            "anomalias detectadas",
            extra={
                "rows_scored": result.rows_scored,
                "anomalies": result.anomalies,
                "anomaly_rate": result.metrics["anomaly_rate"],
                "gold_key": gold_key,
            },
        )
        return result

    def _silver_root(self) -> Path:
        root = (
            self.settings.lake_root / processamento_lake.silver_prefix(self.source)
        ).resolve()
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
            raise FileNotFoundError(
                f"nenhuma linha silver encontrada para symbols={list(tickers)}"
            )
        return frame

    def _write_model(self, run_id: str, pipeline: Any) -> None:
        import joblib

        path = (self.settings.lake_root / processamento_lake.model_key(run_id)).resolve()
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.tmp-{run_id}")
        joblib.dump(pipeline, temporary)
        temporary.replace(path)

    def _write_gold(self, frame: pd.DataFrame, run_id: str) -> str:
        relative = processamento_lake.gold_prefix(self.source)
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
        params: _ModelParams,
        gold_key: str,
    ) -> dict[str, Any]:
        anomalies = int(scored["is_anomaly"].sum())
        holdout = ~mask
        holdout_rows = int(holdout.sum())
        holdout_anomalies = int(scored.loc[holdout, "is_anomaly"].sum()) if holdout_rows else 0
        training = scored.loc[mask]
        scores = scored["anomaly_score"]

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
            "feature_columns": inferencia_features.FEATURE_COLUMNS,
            "params": {
                "contamination": params.contamination,
                "n_estimators": params.n_estimators,
                "random_state": self.settings.anomaly_random_state,
                "train_fraction": params.train_fraction,
                "scaler": "RobustScaler",
                "model": "IsolationForest",
            },
            "score_quantiles": {
                f"p{percentile}": float(np.percentile(scores, percentile))
                for percentile in (50, 90, 95, 99)
            },
            "gold_key": gold_key,
            "model_key": processamento_lake.model_key(run_id),
            "top_anomalies": _top_anomalies(scored),
        }

    def _write_metrics(
        self,
        run_id: str,
        started_at: datetime,
        metrics: dict[str, Any],
    ) -> None:
        key = self._metrics_key(run_id, started_at)
        processamento_artifacts.write_json(self.storage, key, metrics)

    def _write_metadata(
        self,
        run_id: str,
        gold_key: str,
        metrics: dict[str, Any],
    ) -> None:
        metadata = {
            "run_id": run_id,
            "generated_at": metrics["generated_at"],
            "model_key": processamento_lake.model_key(run_id),
            "gold_key": gold_key,
            "features": metrics["feature_columns"],
            "params": metrics["params"],
            "rows_scored": metrics["rows_scored"],
            "anomalies": metrics["anomalies"],
            "anomaly_rate": metrics["anomaly_rate"],
        }
        processamento_artifacts.write_json(
            self.storage, f"{gold_key}_metadata.json", metadata
        )

    def _metrics_key(self, run_id: str, started_at: datetime) -> str:
        return processamento_lake.run_artifact_key(
            run_id, processamento_lake.ingestion_date(started_at), "anomaly_metrics.json"
        )

    def _manifest_key(self, run_id: str, started_at: datetime) -> str:
        return processamento_lake.run_artifact_key(
            run_id, processamento_lake.ingestion_date(started_at), "anomaly_manifest.json"
        )

    def _build_summary(
        self,
        *,
        run_id: str,
        started_at: datetime,
        finished_at: datetime,
        result: _RunResult,
        params: _ModelParams,
    ) -> AnomalySummary:
        started_iso, finished_iso, duration = processamento_artifacts.run_timing(
            started_at, finished_at
        )
        return AnomalySummary(
            run_id=run_id,
            status=result.status,
            started_at=started_iso,
            finished_at=finished_iso,
            duration_seconds=duration,
            manifest_key=self._manifest_key(run_id, started_at),
            metrics_key=self._metrics_key(run_id, started_at),
            model_key=processamento_lake.model_key(run_id),
            gold_prefix=processamento_lake.gold_prefix(self.source),
            totals={
                "rows_input": result.rows_input,
                "rows_scored": result.rows_scored,
                "anomalies": result.anomalies,
                "anomaly_rate": (
                    round(result.anomalies / result.rows_scored, 6)
                    if result.rows_scored
                    else 0.0
                ),
                "symbols": result.symbols_scored,
                "training_rows": result.training_rows,
            },
            config={
                "source": self.source,
                "input": processamento_lake.silver_prefix(self.source),
                "output": processamento_lake.gold_prefix(self.source),
                "contamination": params.contamination,
                "n_estimators": params.n_estimators,
                "train_fraction": params.train_fraction,
                "random_state": self.settings.anomaly_random_state,
            },
            metrics=result.metrics,
            error=result.error,
        )

    def _write_manifest(self, summary: AnomalySummary) -> None:
        processamento_artifacts.write_manifest(
            self.storage,
            summary.manifest_key,
            summary,
            logger=logger,
            failure_message="falha ao gravar manifest de detect",
        )


def _validate_params(params: _ModelParams) -> None:
    if not 0 < params.contamination < 0.5:
        raise ValueError("contamination deve estar entre 0 e 0.5")
    if params.n_estimators < 10:
        raise ValueError("n_estimators deve ser pelo menos 10")
    if not 0 < params.train_fraction <= 1:
        raise ValueError("train_fraction deve estar entre 0 e 1")
