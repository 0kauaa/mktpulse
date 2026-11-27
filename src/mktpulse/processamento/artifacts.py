"""escrita padronizada dos artefatos json gerados por uma execucao."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import Any, Protocol

from .lake import SCHEMA_VERSION
from .storage import Storage


class _SummaryLike(Protocol):
    # protocolo estrutural: qualquer objeto com to_dict() serve, sem heranca.
    def to_dict(self) -> dict[str, Any]: ...

def run_timing(
    started_at: datetime,
    finished_at: datetime,
) -> tuple[str, str, float]:
    """Formato comum de timestamps e duracao nos summaries."""

    return (
        started_at.astimezone(UTC).isoformat(),
        finished_at.astimezone(UTC).isoformat(),
        round((finished_at - started_at).total_seconds(), 3),
    )

def write_json(storage: Storage, key: str, payload: dict[str, Any]) -> None:
    # `default=str` permite serializar path, date e outros tipos sem converter
    # manualmente cada campo do summary.
    body = json.dumps(payload, indent=2, ensure_ascii=False, default=str).encode("utf-8")
    storage.write_bytes(key, body)

def write_manifest(
    storage: Storage,
    key: str,
    summary: _SummaryLike,
    *,
    logger: logging.Logger,
    failure_message: str = "falha ao gravar manifest",
) -> None:
    # o manifest e um artefato operacional: status, contagens, configuracao e erros.
    manifest = {"schema_version": SCHEMA_VERSION, **summary.to_dict()}
    try:
        write_json(storage, key, manifest)
    except Exception:  # noqa: ble001 - manifest nao pode mascarar o resultado
        # falhar ao gravar o manifest nao deve esconder o resultado principal
        # (que pode ter sido sucesso). o erro fica registrado no log.
        logger.exception(failure_message, extra={"manifest_key": key})
