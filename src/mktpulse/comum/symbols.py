"""
normalização de listas de tickers usada por todos os jobs do pipeline.
converte string, sequência ou None em tickers únicos e em maiúsculas.
"""

from __future__ import annotations

from collections.abc import Sequence


def normalize_symbols(symbols: Sequence[str] | str | None) -> list[str]:
    """Converte string, sequência ou ``None`` em tickers únicos e em maiúsculas."""

    if symbols is None:
        return []

    # A CLI recebe "PETR4,VALE3"; a configuração também. Aceitamos os dois
    # formatos e usamos `seen` para preservar a ordem sem duplicar.
    raw = symbols.split(",") if isinstance(symbols, str) else list(symbols)
    seen: set[str] = set()
    normalized: list[str] = []
    for item in raw:
        ticker = item.strip().upper()
        if ticker and ticker not in seen:
            seen.add(ticker)
            normalized.append(ticker)
    return normalized
