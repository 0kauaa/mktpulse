"""Layout do data lake e contrato dos objetos gravados.

Layout:
    raw/source=yahoo/endpoint=quote/ingestion_date=YYYY-MM-DD/symbol=PETR4/part-<hash>.json.gz
    normalized/source=yahoo/table=quotes/symbol=PETR4/quotes.parquet
    _ops/runs/ingestion_date=YYYY-MM-DD/run_id=<id>/manifest.json

Decisoes:
- A camada raw e imutavel e append-only. Ela e a fonte da verdade.
- A chave inclui o hash do payload: reprocessar o mesmo dado no mesmo dia
  sobrescreve exatamente a mesma chave (idempotencia no landing).
- A camada normalized e derivada; pode ser reconstruida a partir do raw.
- `source` participa do path: trocar de fonte nao mistura universos de dados.
- A particao ingestion_date preserva "quando chegou", nao so "a que dia se refere".
  Isso permite auditar dado atrasado ou corrigido.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import math
import uuid
from datetime import UTC, date, datetime
from typing import Any

RAW_ROOT = "raw"
NORMALIZED_ROOT = "normalized"
SILVER_ROOT = "silver"
GOLD_ROOT = "gold"
OPS_ROOT = "_ops/runs"
BENCHMARK_ROOT = "_ops/benchmarks"
MODEL_ROOT = "_ops/models"
SCHEMA_VERSION = 1


def utc_now() -> datetime:
    return datetime.now(UTC)


def ingestion_date(moment: datetime) -> str:
    return moment.astimezone(UTC).date().isoformat()


def raw_prefix(source: str, symbol: str | None = None) -> str:
    base = f"{RAW_ROOT}/source={source}/endpoint=quote/"
    return f"{base}symbol={symbol}/" if symbol else base


def raw_key(source: str, symbol: str, landing_date: str, content_hash: str) -> str:
    return (
        f"{RAW_ROOT}/source={source}/endpoint=quote/"
        f"ingestion_date={landing_date}/symbol={symbol}/part-{content_hash}.json.gz"
    )


def normalized_prefix(source: str) -> str:
    return f"{NORMALIZED_ROOT}/source={source}/table=quotes/"


def normalized_key(source: str, symbol: str) -> str:
    return f"{normalized_prefix(source)}symbol={symbol}/quotes.parquet"


def silver_prefix(source: str) -> str:
    return f"{SILVER_ROOT}/source={source}/table=quotes/"


def gold_prefix(source: str) -> str:
    return f"{GOLD_ROOT}/source={source}/table=anomalies/"


def model_key(run_id: str, name: str = "model.joblib") -> str:
    return f"{MODEL_ROOT}/run_id={run_id}/{name}"


def manifest_key(run_id: str, landing_date: str) -> str:
    return f"{OPS_ROOT}/ingestion_date={landing_date}/run_id={run_id}/manifest.json"


def run_artifact_key(run_id: str, landing_date: str, name: str) -> str:
    return f"{OPS_ROOT}/ingestion_date={landing_date}/run_id={run_id}/{name}"


def benchmark_key(run_id: str) -> str:
    return f"{BENCHMARK_ROOT}/run_id={run_id}/benchmark.json"


def new_run_id(moment: datetime) -> str:
    return f"{moment.astimezone(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:8]}"


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=_json_default
    ).encode("utf-8")


def compute_content_hash(source: str, payload: dict[str, Any]) -> str:
    """Hash do conteudo de negocio, nao dos metadados volateis da cotacao.

    `regularMarketPrice`, `regularMarketTime` etc. mudam a cada request e nao
    representam um novo dado historico. Se entrassem no hash, todo rerun criaria
    um novo objeto raw e a idempotencia seria falsa.
    """
    if source == "brapi":
        material: Any = {
            "symbol": payload.get("symbol"),
            "currency": payload.get("currency"),
            "rows": payload.get("historicalDataPrice"),
        }
    elif source == "yahoo":
        result = payload
        if "chart" in payload:
            results = (payload.get("chart") or {}).get("result") or []
            result = results[0] if results else {}
        meta = result.get("meta") or {}
        material = {
            "symbol": meta.get("symbol"),
            "currency": meta.get("currency"),
            "timestamp": result.get("timestamp"),
            "indicators": result.get("indicators"),
        }
    else:
        material = payload
    return hashlib.sha256(canonical_json(_stable_numbers(material))).hexdigest()


def _stable_numbers(value: Any) -> Any:
    """Arredonda floats para o hash.

    Algumas APIs devolvem o mesmo preco ajustado com ruido de ponto flutuante
    (ex.: 29.969369 vs 29.969372) entre requests. Sem isso, a idempotencia do
    raw seria falsa: cada rerun gravaria um objeto novo para o mesmo dado.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, float):
        return round(value, 4) if math.isfinite(value) else str(value)
    if isinstance(value, dict):
        return {key: _stable_numbers(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_stable_numbers(item) for item in value]
    return value


def build_envelope(
    *,
    source: str,
    run_id: str,
    symbol: str,
    request: dict[str, Any],
    payload: dict[str, Any],
    ingested_at: datetime,
) -> dict[str, Any]:
    content_hash = compute_content_hash(source, payload)
    return {
        "schema_version": SCHEMA_VERSION,
        "source": source,
        "endpoint": "quote",
        "symbol": symbol,
        "run_id": run_id,
        "ingested_at": ingested_at.astimezone(UTC).isoformat(),
        "request": request,
        "content_hash": content_hash,
        "payload": payload,
    }


def encode_envelope(envelope: dict[str, Any]) -> bytes:
    # mtime=0 deixa o gzip deterministico: mesmo envelope -> mesmos bytes.
    return gzip.compress(canonical_json(envelope), compresslevel=6, mtime=0)


def decode_envelope(data: bytes) -> dict[str, Any]:
    try:
        envelope = json.loads(gzip.decompress(data).decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"raw envelope invalido: {exc}") from exc
    if (
        not isinstance(envelope, dict)
        or "payload" not in envelope
        or "content_hash" not in envelope
    ):
        raise ValueError("raw envelope sem payload/content_hash")
    return envelope


def date_from_epoch(value: Any) -> date:
    """Converte o epoch da brapi para a data do pregao em Sao Paulo.

    A brapi envia timestamps de meia-noite de Brasilia (03:00 UTC). Converter
    para UTC funcionaria por coincidencia; converter para o fuso da B3 expressa
    a regra de negocio correta.
    """
    from zoneinfo import ZoneInfo

    try:
        timestamp = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"timestamp invalido: {value!r}") from exc
    if timestamp > 10_000_000_000:  # heuristica para epoch em milissegundos
        timestamp //= 1000
    return (
        datetime.fromtimestamp(timestamp, tz=UTC).astimezone(ZoneInfo("America/Sao_Paulo")).date()
    )


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"tipo nao serializavel: {type(value)!r}")
