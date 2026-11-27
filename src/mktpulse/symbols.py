"""normalizacao de listas de tickers usada por todos os jobs do pipeline."""

from __future__ import annotations
from collections.abc import Sequence

def normalize_symbols(symbols: Sequence[str] | str | None) -> list[str]:
    """converte string, sequencia ou None em tickers únicos e em maiusculas."""

    if symbols is None:
        return []

    # a cli recebe "petr4,vale3" e a configuracao tambem
    raw = symbols.split(",") if isinstance(symbols, str) else list(symbols)
    seen: set[str] = set()
    normalized: list[str] = []
    for item in raw:
        ticker = item.strip().upper()
        if ticker and ticker not in seen:
            seen.add(ticker)
            normalized.append(ticker)
    return normalized
