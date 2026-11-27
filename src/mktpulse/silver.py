"""
job spark que constroi a camada silver a partir do normalized.
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

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Settings
from .indicators import add_indicators
from .lake import (
    SCHEMA_VERSION,
    ingestion_date,
    manifest_key,
    new_run_id,
    normalized_prefix,
    run_artifact_key,
    silver_prefix,
    utc_now,
)
from .observability import run_id_var
from .quality import QualityReport, evaluate_quality
from .spark_schemas import QUOTES_SPARK_SCHEMA
from .spark_session import build_spark_session
from .storage import Storage

logger = logging.getLogger("mktpulse.silver")

@dataclass
class SilverSummary:
    run_id: str
    status: str
    started_at: str
    finished_at: str
    duration_seconds: float
    manifest_key: str
    quality_report_key: str
    silver_prefix: str
    totals: dict[str, Any]
    config: dict[str, Any]
    quality: dict[str, Any] = field(default_factory=dict)
    operation: str = "transform"
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

def _quality_summary(report: QualityReport | None) -> dict[str, Any]:
    if report is None:
        return {}
    return {
        "status": report.status,
        "total_rows": report.total_rows,
        "critical_failures": report.critical_failures,
        "warning_failures": report.warning_failures,
        "rules": [
            {
                "name": rule.name,
                "severity": rule.severity,
                "failed_rows": rule.failed_rows,
                "passed": rule.passed,
            }
            for rule in report.rules
        ],
    }

class SilverJob:
    def __init__(
        self,
        settings: Settings,
        storage: Storage,
        *,
        spark: SparkSession | None = None,
        master: str | None = None,
        shuffle_partitions: int | None = None,
        now: Any = utc_now,
    ) -> None:
        self.settings = settings
        self.source = settings.source
        self.storage = storage
        self.spark = spark
        self.master = master
        self.shuffle_partitions = shuffle_partitions
        self._now = now

    def run(
        self,
        symbols: Sequence[str] | str | None = None,
        *,
        run_id: str | None = None,
        full_refresh: bool = False,
    ) -> SilverSummary:
        if self.settings.lake_backend != "local":
            raise RuntimeError(
                "transform com Spark esta implementado apenas para lake local nesta semana"
            )

        started_at = self._now()
        run_id = run_id or new_run_id(started_at)
        token = run_id_var.set(run_id)
        tickers = _normalize_symbols(symbols)
        quality_report: QualityReport | None = None
        enriched: DataFrame | None = None
        output_path: Path | None = None
        rows_read = 0
        rows_written = 0
        symbol_count = 0
        partitions_written = 0
        bytes_written = 0
        status = "failed"
        fatal_error: Exception | None = None
        error: str | None = None
        spark = self.spark
        spark_owned = False

        logger.info(
            "transform iniciado",
            extra={
                "source": self.source,
                "symbols": tickers or "todos",
                "full_refresh": full_refresh,
            },
        )

        try:
            if spark is None:
                spark = build_spark_session(
                    app_name=f"mktpulse-transform-{run_id}",
                    master=self.master or self.settings.spark_master,
                    shuffle_partitions=(
                        self.shuffle_partitions or self.settings.spark_shuffle_partitions
                    ),
                )
                spark_owned = True

            bronze = self._read_bronze(spark, tickers)
            enriched = add_indicators(bronze).cache()
            quality_report = evaluate_quality(
                enriched,
                as_of=started_at.date(),
                max_daily_return=self.settings.quality_max_daily_return,
                max_gap_days=self.settings.quality_max_gap_days,
                max_staleness_days=self.settings.quality_max_staleness_days,
            )
            rows_read = quality_report.total_rows
            symbol_count = enriched.select("symbol").distinct().count()
            self._write_quality_report(run_id, started_at, quality_report)

            if quality_report.status == "failed":
                error = (
                    f"qualidade bloqueou o silver: "
                    f"{quality_report.critical_failures} linha(s) em regras criticas"
                )
                logger.error(
                    "qualidade bloqueou o silver",
                    extra={
                        "critical_failures": quality_report.critical_failures,
                        "quality_report_key": self._quality_report_key(run_id, started_at),
                    },
                )
            else:
                output_path = self._output_path()
                if full_refresh and output_path.exists():
                    shutil.rmtree(output_path)

                processed_at = self._now()
                self._write_silver(
                    enriched,
                    output_path,
                    run_id=run_id,
                    processed_at=processed_at,
                )
                rows_written = spark.read.parquet(str(output_path)).count()
                partitions_written = (
                    spark.read.parquet(str(output_path))
                    .select("symbol", "year", "month")
                    .distinct()
                    .count()
                )
                bytes_written = sum(path.stat().st_size for path in output_path.rglob("*.parquet"))
                status = "success"
                logger.info(
                    "silver escrito",
                    extra={
                        "rows_written": rows_written,
                        "partitions": partitions_written,
                        "bytes_written": bytes_written,
                        "silver_path": str(output_path),
                    },
                )
        except Exception as exc:  # noqa: ble001 - garante manifest e status failure
            fatal_error = exc
            status = "failed"
            error = str(exc)
            logger.exception("transform abortado")
        finally:
            finished_at = self._now()
            summary = self._build_summary(
                run_id=run_id,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                quality_report=quality_report,
                rows_read=rows_read,
                rows_written=rows_written,
                symbol_count=symbol_count,
                partitions_written=partitions_written,
                bytes_written=bytes_written,
                tickers=tickers,
                full_refresh=full_refresh,
                error=error,
            )
            self._write_manifest(summary)
            if enriched is not None:
                enriched.unpersist()
            if spark_owned and spark is not None:
                spark.stop()
            run_id_var.reset(token)

        if fatal_error:
            raise fatal_error
        return summary

    def _read_bronze(self, spark: SparkSession, tickers: Sequence[str]) -> DataFrame:
        root = (self.settings.lake_root / normalized_prefix(self.source)).resolve()
        if not root.exists() or next(root.rglob("*.parquet"), None) is None:
            raise FileNotFoundError(
                f"nenhum Parquet normalized encontrado em {root}; rode o rebuild primeiro"
            )

        frame = spark.read.schema(QUOTES_SPARK_SCHEMA).parquet(str(root))
        if tickers:
            frame = frame.filter(F.col("symbol").isin(tickers))
        if not frame.take(1):
            raise FileNotFoundError(f"nenhuma linha normalized encontrada para symbols={tickers}")
        return frame

    def _output_path(self) -> Path:
        return (self.settings.lake_root / silver_prefix(self.source)).resolve()

    def _write_silver(
        self,
        frame: DataFrame,
        output_path: Path,
        *,
        run_id: str,
        processed_at: datetime,
    ) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        prepared = (
            frame.withColumn("year", F.year("trade_date"))
            .withColumn("month", F.month("trade_date"))
            .withColumn("processing_run_id", F.lit(run_id))
            .withColumn("processed_at", F.lit(processed_at))
            .repartition("symbol", "year", "month")
            .sortWithinPartitions("trade_date")
        )
        prepared.write.mode("overwrite").partitionBy("symbol", "year", "month").parquet(
            str(output_path)
        )

    def _quality_report_key(self, run_id: str, started_at: datetime) -> str:
        return run_artifact_key(run_id, ingestion_date(started_at), "quality_report.json")

    def _write_quality_report(
        self,
        run_id: str,
        started_at: datetime,
        report: QualityReport,
    ) -> None:
        key = self._quality_report_key(run_id, started_at)
        body = json.dumps(report.to_dict(), indent=2, ensure_ascii=False, default=str).encode(
            "utf-8"
        )
        self.storage.write_bytes(key, body)

    def _build_summary(
        self,
        *,
        run_id: str,
        status: str,
        started_at: datetime,
        finished_at: datetime,
        quality_report: QualityReport | None,
        rows_read: int,
        rows_written: int,
        symbol_count: int,
        partitions_written: int,
        bytes_written: int,
        tickers: Sequence[str],
        full_refresh: bool,
        error: str | None,
    ) -> SilverSummary:
        quality = _quality_summary(quality_report)
        totals = {
            "rows_read": rows_read,
            "rows_written": rows_written,
            "symbols": symbol_count,
            "partitions_written": partitions_written,
            "bytes_written": bytes_written,
            "critical_failures": quality.get("critical_failures", 0),
            "warnings": quality.get("warning_failures", 0),
        }
        return SilverSummary(
            run_id=run_id,
            status=status,
            started_at=started_at.astimezone(UTC).isoformat(),
            finished_at=finished_at.astimezone(UTC).isoformat(),
            duration_seconds=round((finished_at - started_at).total_seconds(), 3),
            manifest_key=manifest_key(run_id, ingestion_date(started_at)),
            quality_report_key=self._quality_report_key(run_id, started_at),
            silver_prefix=silver_prefix(self.source),
            totals=totals,
            config={
                "source": self.source,
                "input": normalized_prefix(self.source),
                "output": silver_prefix(self.source),
                "symbols": list(tickers),
                "full_refresh": full_refresh,
                "master": self.master or self.settings.spark_master,
                "shuffle_partitions": (
                    self.shuffle_partitions or self.settings.spark_shuffle_partitions
                ),
            },
            quality=quality,
            error=error,
        )

    def _write_manifest(self, summary: SilverSummary) -> None:
        manifest = {
            "schema_version": SCHEMA_VERSION,
            **summary.to_dict(),
        }
        body = json.dumps(manifest, indent=2, ensure_ascii=False, default=str).encode("utf-8")
        try:
            self.storage.write_bytes(summary.manifest_key, body)
        except Exception:  # noqa: ble001 - manifest nao pode mascarar o resultado
            logger.exception(
                "falha ao gravar manifest de transform",
                extra={"manifest_key": summary.manifest_key},
            )
