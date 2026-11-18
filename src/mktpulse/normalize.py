"""Normalizacao e validacao basica do payload OHLCV.

Cada fonte tem seu formato; o contrato de saida e unico (QUOTES_COLUMNS).
A qualidade "de verdade" (regras de negocio, outliers, comparacao entre fontes)
fica para a Semana 2. Aqui garantimos tipos, unicidade por data e invariantes OHLC.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import pandas as pd
import pyarrow as pa

from .lake import date_from_epoch

QUOTES_COLUMNS = [
    "symbol",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "adjusted_close",
    "volume",
    "currency",
    "source",
    "ingested_at",
    "run_id",
    "content_hash",
]

QUOTES_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string()),
        pa.field("trade_date", pa.date32()),
        pa.field("open", pa.float64()),
        pa.field("high", pa.float64()),
        pa.field("low", pa.float64()),
        pa.field("close", pa.float64()),
        pa.field("adjusted_close", pa.float64()),
        pa.field("volume", pa.int64()),
        pa.field("currency", pa.string()),
        pa.field("source", pa.string()),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
        pa.field("run_id", pa.string()),
        pa.field("content_hash", pa.string()),
    ]
)


class DataQualityError(ValueError):
    """O payload nao respeita o contrato minimo e nao deve entrar no normalized."""


@dataclass(frozen=True)
class _Context:
    source: str
    run_id: str
    content_hash: str
    ingested_at: pd.Timestamp


def _context(envelope: dict[str, Any]) -> _Context:
    ingested_at = pd.Timestamp(envelope["ingested_at"])
    ingested_at = (
        ingested_at.tz_localize("UTC")
        if ingested_at.tzinfo is None
        else ingested_at.tz_convert("UTC")
    )
    return _Context(
        source=str(envelope.get("source", "")).lower(),
        run_id=str(envelope.get("run_id", "")),
        content_hash=str(envelope.get("content_hash", "")),
        ingested_at=ingested_at,
    )


def _as_float(value: Any, *, field: str, symbol: str, position: int) -> float:
    if value is None:
        raise DataQualityError(f"{symbol}: campo {field} ausente na linha {position}")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise DataQualityError(f"{symbol}: campo {field} invalido na linha {position}") from exc
    if not math.isfinite(number):
        raise DataQualityError(f"{symbol}: campo {field} nao finito na linha {position}")
    return number


def _as_volume(value: Any, *, symbol: str, position: int) -> int:
    if value is None:
        raise DataQualityError(f"{symbol}: campo volume ausente na linha {position}")
    try:
        volume = int(value)
    except (TypeError, ValueError) as exc:
        raise DataQualityError(f"{symbol}: campo volume invalido na linha {position}") from exc
    if volume < 0:
        raise DataQualityError(f"{symbol}: volume negativo na linha {position}")
    return volume


def _make_row(
    *,
    symbol: str,
    trade_date: Any,
    open_: Any,
    high: Any,
    low: Any,
    close: Any,
    adjusted_close: Any,
    volume: Any,
    currency: str,
    context: _Context,
    position: int,
) -> dict[str, Any]:
    open_ = _as_float(open_, field="open", symbol=symbol, position=position)
    high = _as_float(high, field="high", symbol=symbol, position=position)
    low = _as_float(low, field="low", symbol=symbol, position=position)
    close = _as_float(close, field="close", symbol=symbol, position=position)
    adjusted_close = (
        close
        if adjusted_close is None
        else _as_float(adjusted_close, field="adjusted_close", symbol=symbol, position=position)
    )
    volume = _as_volume(volume, symbol=symbol, position=position)

    if min(open_, high, low, close, adjusted_close) <= 0:
        raise DataQualityError(f"{symbol}: preco nao positivo na linha {position}")
    if high < low:
        raise DataQualityError(f"{symbol}: high < low na linha {position}")
    eps = 1e-9
    if high + eps < max(open_, close) or low - eps > min(open_, close):
        raise DataQualityError(
            f"{symbol}: OHLC inconsistente em {trade_date} "
            f"(open={open_}, high={high}, low={low}, close={close})"
        )

    return {
        "symbol": symbol,
        "trade_date": trade_date,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "adjusted_close": adjusted_close,
        "volume": volume,
        "currency": currency,
        "source": context.source,
        "ingested_at": context.ingested_at,
        "run_id": context.run_id,
        "content_hash": context.content_hash,
    }


def normalize_envelope(envelope: dict[str, Any]) -> pd.DataFrame:
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        raise DataQualityError("payload ausente ou invalido")

    context = _context(envelope)
    if context.source == "brapi":
        rows, symbol, received, skipped = _normalize_brapi(envelope, payload, context)
    elif context.source == "yahoo":
        rows, symbol, received, skipped = _normalize_yahoo(envelope, payload, context)
    else:
        raise DataQualityError(f"fonte sem normalizador: {context.source or 'desconhecida'}")

    if not rows:
        raise DataQualityError(f"{symbol}: nenhuma linha valida no payload")

    frame = pd.DataFrame(rows, columns=QUOTES_COLUMNS)
    frame = (
        frame.drop_duplicates(subset=["trade_date"], keep="last")
        .sort_values("trade_date")
        .reset_index(drop=True)
    )
    frame.attrs["rows_received"] = received
    frame.attrs["rows_skipped"] = skipped
    return frame[QUOTES_COLUMNS]


def _normalize_brapi(
    envelope: dict[str, Any], payload: dict[str, Any], context: _Context
) -> tuple[list[dict[str, Any]], str, int, int]:
    symbol = str(payload.get("symbol") or envelope.get("symbol") or "").upper()
    if not symbol:
        raise DataQualityError("payload sem symbol")

    history = payload.get("historicalDataPrice") or []
    if not isinstance(history, list) or not history:
        raise DataQualityError(f"{symbol}: historicalDataPrice vazio")

    currency = str(payload.get("currency") or "BRL")
    rows: list[dict[str, Any]] = []
    for position, item in enumerate(history):
        if not isinstance(item, dict):
            raise DataQualityError(f"{symbol}: linha {position} nao e um objeto")
        try:
            trade_date = date_from_epoch(item.get("date"))
        except ValueError as exc:
            raise DataQualityError(f"{symbol}: data invalida na linha {position}") from exc
        rows.append(
            _make_row(
                symbol=symbol,
                trade_date=trade_date,
                open_=item.get("open"),
                high=item.get("high"),
                low=item.get("low"),
                close=item.get("close"),
                adjusted_close=item.get("adjustedClose"),
                volume=item.get("volume"),
                currency=currency,
                context=context,
                position=position,
            )
        )
    return rows, symbol, len(history), 0


def _at(values: Any, index: int) -> Any:
    if not isinstance(values, list) or index >= len(values):
        return None
    return values[index]


def _normalize_yahoo(
    envelope: dict[str, Any], payload: dict[str, Any], context: _Context
) -> tuple[list[dict[str, Any]], str, int, int]:
    # Contrato do raw: payload e o chart.result do ticker. Aceitamos o envelope
    # completo do endpoint tambem, para facilitar replay de capturas manuais.
    if "chart" in payload:
        chart = payload.get("chart") or {}
        results = chart.get("result") or []
        if not results:
            error = chart.get("error") or {}
            detail = error.get("description") if isinstance(error, dict) else None
            raise DataQualityError(f"yahoo sem chart.result: {detail or 'sem detalhe'}")
        result = results[0]
    else:
        result = payload

    meta = result.get("meta") or {}
    symbol = str(meta.get("symbol") or envelope.get("symbol") or "").upper()
    if symbol.endswith(".SA"):
        symbol = symbol[:-3]
    if not symbol:
        raise DataQualityError("payload yahoo sem symbol")

    timestamps = result.get("timestamp") or []
    indicators = result.get("indicators") or {}
    quote_list = indicators.get("quote") or []
    if not timestamps or not quote_list:
        raise DataQualityError(f"{symbol}: yahoo sem timestamps/quote")

    quote = quote_list[0]
    adjclose_list = indicators.get("adjclose") or []
    adjclose = adjclose_list[0].get("adjclose") if adjclose_list else None
    currency = str(meta.get("currency") or "BRL")

    rows: list[dict[str, Any]] = []
    skipped = 0
    for position, timestamp in enumerate(timestamps):
        open_ = _at(quote.get("open"), position)
        high = _at(quote.get("high"), position)
        low = _at(quote.get("low"), position)
        close = _at(quote.get("close"), position)
        volume = _at(quote.get("volume"), position)

        # Barra incompleta (pregao em andamento, feriado, ativo suspenso):
        # nao entra no historico diario. A contagem fica no manifest.
        if any(value is None for value in (open_, high, low, close, volume)):
            skipped += 1
            continue
        if min(open_, high, low, close) <= 0 or volume <= 0:
            skipped += 1
            continue

        try:
            trade_date = date_from_epoch(timestamp)
        except ValueError as exc:
            raise DataQualityError(f"{symbol}: data invalida na linha {position}") from exc
        adjusted = _at(adjclose, position)
        rows.append(
            _make_row(
                symbol=symbol,
                trade_date=trade_date,
                open_=open_,
                high=high,
                low=low,
                close=close,
                adjusted_close=adjusted,
                volume=volume,
                currency=currency,
                context=context,
                position=position,
            )
        )
    return rows, symbol, len(timestamps), skipped
