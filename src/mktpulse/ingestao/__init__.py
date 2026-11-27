"""Ingestão: captura dos dados das fontes (Brapi e Yahoo)."""

from .pipeline import (
    IngestionPipeline,
    RunSummary,
    read_snapshot,
    SymbolOutcome,
)

__all__ = [
    "IngestionPipeline",
    "RunSummary",
    "read_snapshot",
    "SymbolOutcome",
]
