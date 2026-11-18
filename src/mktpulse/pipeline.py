"""Pipeline de ingestao e replay da Semana 1.

Garantias operacionais:
- falha de um ticker nao derruba os demais (isolamento por simbolo);
- landing raw idempotente por (dia, ticker, hash do payload);
- normalized sempre deduplicado por (ticker, data do pregao), vencendo a
  ingestao mais recente;
- toda execucao grava um manifest com status, contagens e erros;
- o normalized pode ser reconstruido a partir do raw (replay).
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from io import BytesIO
from typing import Any

import pandas as pd

from .config import Settings
from .lake import (
    SCHEMA_VERSION,
    build_envelope,
    decode_envelope,
    encode_envelope,
    ingestion_date,
    manifest_key,
    normalized_key,
    raw_key,
    raw_prefix,
    utc_now,
)
from .normalize import QUOTES_COLUMNS, QUOTES_SCHEMA, normalize_envelope
from .observability import run_id_var
from .sources import QuoteSource
from .storage import Storage

logger = logging.getLogger("mktpulse.pipeline")


@dataclass
class SymbolOutcome:
    symbol: str
    status: str = "ok"
    rows_received: int = 0
    rows_written: int = 0
    rows_stored: int = 0
    rows_skipped: int = 0
    raw_key: str | None = None
    raw_bytes: int = 0
    raw_written: bool = False
    content_hash: str | None = None
    error: str | None = None


@dataclass
class RunSummary:
    run_id: str
    operation: str
    status: str
    started_at: str
    finished_at: str
    duration_seconds: float
    manifest_key: str
    symbols: dict[str, SymbolOutcome]
    totals: dict[str, int]
    config: dict[str, Any] = field(default_factory=dict)
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


def _chunks(items: Sequence[str], size: int) -> list[list[str]]:
    return [list(items[index : index + size]) for index in range(0, len(items), size)]


def _dedupe_quotes(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame[QUOTES_COLUMNS].copy()
    frame = frame.sort_values(["trade_date", "ingested_at"], kind="stable")
    frame = frame.drop_duplicates(subset=["symbol", "trade_date"], keep="last")
    return frame.sort_values(["symbol", "trade_date"]).reset_index(drop=True)


def _read_snapshot(storage: Storage, source: str, symbol: str) -> pd.DataFrame | None:
    key = normalized_key(source, symbol)
    if not storage.exists(key):
        return None
    return pd.read_parquet(BytesIO(storage.read_bytes(key)))


def read_snapshot(storage: Storage, source: str, symbol: str) -> pd.DataFrame:
    frame = _read_snapshot(storage, source, symbol.upper())
    if frame is None:
        raise FileNotFoundError(
            f"snapshot normalizado nao encontrado para {symbol.upper()} em source={source}"
        )
    return frame


class IngestionPipeline:
    def __init__(
        self,
        settings: Settings,
        client: QuoteSource | None,
        storage: Storage,
        *,
        now: Any = utc_now,
    ) -> None:
        self.settings = settings
        self.source = settings.source
        self.client = client
        self.storage = storage
        self._now = now

    def ingest(
        self,
        symbols: Sequence[str] | str | None = None,
        *,
        range_: str = "1y",
        interval: str = "1d",
        run_id: str | None = None,
    ) -> RunSummary:
        if self.client is None:
            raise RuntimeError("fonte de dados nao configurada para ingestao")
        tickers = _normalize_symbols(symbols or self.settings.symbol_list)
        if not tickers:
            raise ValueError("informe pelo menos um ticker")

        started_at = self._now()
        run_id = run_id or self._new_run_id(started_at)
        token = run_id_var.set(run_id)
        outcomes = {symbol: SymbolOutcome(symbol=symbol) for symbol in tickers}
        fatal_error: Exception | None = None
        status = "failed"

        batch_size = self.settings.brapi_batch_size if self.source == "brapi" else 1
        logger.info(
            "ingestao iniciada",
            extra={
                "source": self.source,
                "range": range_,
                "interval": interval,
                "symbols": tickers,
            },
        )
        try:
            for index, batch in enumerate(_chunks(tickers, batch_size)):
                if index:
                    time.sleep(self.settings.request_pause_seconds)
                try:
                    response = self.client.fetch_quotes(batch, range_=range_, interval=interval)
                except Exception as exc:  # noqa: BLE001 - erro de batch vira erro por simbolo
                    logger.error(
                        "falha ao buscar lote",
                        extra={"symbols": batch, "error": str(exc)},
                        exc_info=True,
                    )
                    for symbol in batch:
                        outcomes[symbol].status = "error"
                        outcomes[symbol].error = str(exc)
                    continue

                results = response
                for symbol in batch:
                    result = results.get(symbol)
                    if result is None:
                        outcomes[symbol].status = "error"
                        outcomes[symbol].error = "simbolo ausente na resposta da fonte"
                        logger.error("simbolo ausente na resposta", extra={"symbol": symbol})
                        continue
                    try:
                        outcomes[symbol] = self._ingest_symbol(
                            symbol=symbol,
                            result=result,
                            batch=batch,
                            range_=range_,
                            interval=interval,
                            run_id=run_id,
                        )
                    except Exception as exc:  # noqa: BLE001 - isolamento por simbolo
                        outcomes[symbol].status = "error"
                        outcomes[symbol].error = str(exc)
                        logger.exception("falha ao ingerir simbolo", extra={"symbol": symbol})
        except Exception as exc:  # noqa: BLE001 - garante manifest em falha fatal
            fatal_error = exc
            logger.exception("ingestao abortada")
        finally:
            finished_at = self._now()
            status = self._status(outcomes, fatal_error)
            totals = self._totals(outcomes)
            summary = self._build_summary(
                run_id=run_id,
                operation="ingest",
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                outcomes=outcomes,
                totals=totals,
                config={
                    "source": self.source,
                    "range": range_,
                    "interval": interval,
                    "symbols": tickers,
                },
                error=str(fatal_error) if fatal_error else None,
            )
            self._write_manifest(summary)
            logger.info("ingestao finalizada", extra={"status": status, **totals})
            run_id_var.reset(token)

        if fatal_error:
            raise fatal_error
        return summary

    def rebuild(
        self,
        symbols: Sequence[str] | str | None = None,
        *,
        run_id: str | None = None,
    ) -> RunSummary:
        started_at = self._now()
        run_id = run_id or self._new_run_id(started_at)
        token = run_id_var.set(run_id)
        target: list[str] = []
        outcomes: dict[str, SymbolOutcome] = {}
        fatal_error: Exception | None = None
        status = "failed"

        logger.info("rebuild iniciado", extra={"symbols": _normalize_symbols(symbols)})
        try:
            target = self._resolve_rebuild_symbols(symbols)
            outcomes = {symbol: SymbolOutcome(symbol=symbol) for symbol in target}
            for symbol in target:
                try:
                    outcomes[symbol] = self._rebuild_symbol(symbol, run_id=run_id)
                except Exception as exc:  # noqa: BLE001 - isolamento por simbolo
                    outcomes[symbol].status = "error"
                    outcomes[symbol].error = str(exc)
                    logger.exception("falha no rebuild do simbolo", extra={"symbol": symbol})
        except Exception as exc:  # noqa: BLE001 - garante manifest em falha fatal
            fatal_error = exc
            logger.exception("rebuild abortado")
        finally:
            finished_at = self._now()
            status = self._status(outcomes, fatal_error)
            totals = self._totals(outcomes)
            summary = self._build_summary(
                run_id=run_id,
                operation="rebuild",
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                outcomes=outcomes,
                totals=totals,
                config={"source": self.source, "input": "raw", "symbols": target},
                error=str(fatal_error) if fatal_error else None,
            )
            self._write_manifest(summary)
            logger.info("rebuild finalizado", extra={"status": status, **totals})
            run_id_var.reset(token)

        if fatal_error:
            raise fatal_error
        return summary

    def _ingest_symbol(
        self,
        *,
        symbol: str,
        result: dict[str, Any],
        batch: Sequence[str],
        range_: str,
        interval: str,
        run_id: str,
    ) -> SymbolOutcome:
        ingested_at = self._now()
        envelope = build_envelope(
            source=self.source,
            run_id=run_id,
            symbol=symbol,
            request={"range": range_, "interval": interval, "tickers": list(batch)},
            payload=result,
            ingested_at=ingested_at,
        )
        content_hash = str(envelope["content_hash"])
        key = raw_key(self.source, symbol, ingestion_date(ingested_at), content_hash)
        encoded = encode_envelope(envelope)

        raw_written = False
        if self.storage.exists(key):
            logger.info(
                "raw ja existia; escrita ignorada (idempotencia)",
                extra={"symbol": symbol, "raw_key": key},
            )
        else:
            self.storage.write_bytes(key, encoded)
            raw_written = True

        outcome = SymbolOutcome(
            symbol=symbol,
            raw_key=key,
            raw_bytes=len(encoded),
            raw_written=raw_written,
            content_hash=content_hash,
        )
        try:
            normalized = normalize_envelope(envelope)
            outcome.rows_received = int(normalized.attrs.get("rows_received", len(normalized)))
            outcome.rows_skipped = int(normalized.attrs.get("rows_skipped", 0))
            outcome.rows_written = len(normalized)
            outcome.rows_stored = self._merge_and_write(symbol, normalized)
        except Exception as exc:  # noqa: BLE001 - raw fica preservado para diagnostico
            outcome.status = "error"
            outcome.error = str(exc)
            logger.exception(
                "payload raw preservado, mas falhou na normalizacao",
                extra={"symbol": symbol, "raw_key": key, "error": str(exc)},
            )
            return outcome

        logger.info(
            "simbolo ingerido",
            extra={
                "symbol": symbol,
                "rows_received": outcome.rows_received,
                "rows_skipped": outcome.rows_skipped,
                "rows_normalized": outcome.rows_written,
                "rows_stored": outcome.rows_stored,
                "raw_key": key,
                "raw_written": raw_written,
            },
        )
        return outcome

    def _merge_and_write(self, symbol: str, frame: pd.DataFrame) -> int:
        existing = _read_snapshot(self.storage, self.source, symbol)
        combined = frame if existing is None else pd.concat([existing, frame], ignore_index=True)
        combined = _dedupe_quotes(combined)
        self._write_snapshot(symbol, combined)
        return len(combined)

    def _write_snapshot(self, symbol: str, frame: pd.DataFrame) -> None:
        buffer = BytesIO()
        frame.to_parquet(buffer, index=False, engine="pyarrow", schema=QUOTES_SCHEMA)
        self.storage.write_bytes(normalized_key(self.source, symbol), buffer.getvalue())

    def _rebuild_symbol(self, symbol: str, *, run_id: str) -> SymbolOutcome:
        keys = [
            key for key in self.storage.list(raw_prefix(self.source)) if f"/symbol={symbol}/" in key
        ]
        if not keys:
            raise FileNotFoundError(f"nenhum objeto raw encontrado para {symbol}")

        frames: list[pd.DataFrame] = []
        errors: list[str] = []
        for key in keys:
            try:
                envelope = decode_envelope(self.storage.read_bytes(key))
                frames.append(normalize_envelope(envelope))
            except Exception as exc:  # noqa: BLE001 - agrega erros para nao sobrescrever snapshot
                errors.append(f"{key}: {exc}")
                logger.error(
                    "raw invalido durante rebuild", extra={"raw_key": key, "error": str(exc)}
                )

        if errors:
            raise RuntimeError(
                f"{len(errors)} objeto(s) raw invalido(s); snapshot de {symbol} preservado"
            )

        combined = _dedupe_quotes(pd.concat(frames, ignore_index=True))
        self._write_snapshot(symbol, combined)
        rows_received = sum(int(frame.attrs.get("rows_received", len(frame))) for frame in frames)
        rows_skipped = sum(int(frame.attrs.get("rows_skipped", 0)) for frame in frames)
        logger.info(
            "simbolo reconstruido",
            extra={
                "symbol": symbol,
                "raw_objects": len(keys),
                "rows_received": rows_received,
                "rows_skipped": rows_skipped,
                "rows_stored": len(combined),
                "run_id": run_id,
            },
        )
        return SymbolOutcome(
            symbol=symbol,
            status="ok",
            rows_received=rows_received,
            rows_written=rows_received,
            rows_stored=len(combined),
            rows_skipped=rows_skipped,
        )

    def _resolve_rebuild_symbols(self, symbols: Sequence[str] | str | None) -> list[str]:
        requested = _normalize_symbols(symbols)
        if requested:
            return requested

        discovered: set[str] = set()
        for key in self.storage.list(raw_prefix(self.source)):
            for part in key.split("/"):
                if part.startswith("symbol="):
                    discovered.add(part.split("=", 1)[1].upper())
        if not discovered:
            raise FileNotFoundError("nenhum dado raw no lake para reconstruir")
        return sorted(discovered)

    def _build_summary(
        self,
        *,
        run_id: str,
        operation: str,
        status: str,
        started_at: datetime,
        finished_at: datetime,
        outcomes: dict[str, SymbolOutcome],
        totals: dict[str, int],
        config: dict[str, Any],
        error: str | None,
    ) -> RunSummary:
        return RunSummary(
            run_id=run_id,
            operation=operation,
            status=status,
            started_at=started_at.astimezone(UTC).isoformat(),
            finished_at=finished_at.astimezone(UTC).isoformat(),
            duration_seconds=round((finished_at - started_at).total_seconds(), 3),
            manifest_key=manifest_key(run_id, ingestion_date(started_at)),
            symbols=outcomes,
            totals=totals,
            config=config,
            error=error,
        )

    def _write_manifest(self, summary: RunSummary) -> None:
        manifest = {
            "schema_version": SCHEMA_VERSION,
            **summary.to_dict(),
        }
        body = json.dumps(manifest, indent=2, ensure_ascii=False, default=str).encode("utf-8")
        try:
            self.storage.write_bytes(summary.manifest_key, body)
        except Exception:  # noqa: BLE001 - manifest nao pode mascarar o resultado
            logger.exception(
                "falha ao gravar manifest", extra={"manifest_key": summary.manifest_key}
            )

    @staticmethod
    def _status(outcomes: dict[str, SymbolOutcome], fatal_error: Exception | None) -> str:
        if fatal_error or not outcomes:
            return "failed"
        failed = [outcome for outcome in outcomes.values() if outcome.status != "ok"]
        if len(failed) == len(outcomes):
            return "failed"
        return "partial" if failed else "success"

    @staticmethod
    def _totals(outcomes: dict[str, SymbolOutcome]) -> dict[str, int]:
        values = list(outcomes.values())
        return {
            "symbols_total": len(values),
            "symbols_ok": sum(1 for item in values if item.status == "ok"),
            "symbols_failed": sum(1 for item in values if item.status != "ok"),
            "rows_received": sum(item.rows_received for item in values),
            "rows_written": sum(item.rows_written for item in values),
            "rows_stored": sum(item.rows_stored for item in values),
            "rows_skipped": sum(item.rows_skipped for item in values),
            "raw_objects_written": sum(1 for item in values if item.raw_written),
            "raw_objects_skipped": sum(
                1 for item in values if item.raw_key and not item.raw_written
            ),
            "raw_bytes_written": sum(item.raw_bytes for item in values if item.raw_written),
        }

    @staticmethod
    def _new_run_id(moment: datetime) -> str:
        return f"{moment.astimezone(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"
