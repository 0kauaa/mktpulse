"""Logs estruturados em JSON.

Cada linha de log carrega o run_id da execucao corrente. Isso e o que permite
correlacionar uma execucao agendada com os objetos gravados no data lake.
"""

from __future__ import annotations

import contextvars
import json
import logging
import sys
from datetime import UTC, datetime

run_id_var: contextvars.ContextVar[str] = contextvars.ContextVar("run_id", default="-")

_STANDARD_KEYS = set(logging.makeLogRecord({}).__dict__) | {"asctime", "message"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": run_id_var.get(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_KEYS and key not in payload and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def setup_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.setLevel(level.upper())

    for handler in root.handlers:
        if getattr(handler, "_mktpulse_json", False):
            handler.setLevel(level.upper())
            return

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    handler.setLevel(level.upper())
    handler._mktpulse_json = True  # type: ignore[attr-defined]
    root.addHandler(handler)
