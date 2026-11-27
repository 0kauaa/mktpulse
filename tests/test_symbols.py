from __future__ import annotations

from mktpulse.symbols import normalize_symbols

def test_normalize_symbols_accepts_string_and_sequence() -> None:
    assert normalize_symbols(" petr4, VALE3,,petr4 ") == ["PETR4", "VALE3"]
    assert normalize_symbols(["vale3", " PETR4 ", "vale3"]) == ["VALE3", "PETR4"]

def test_normalize_symbols_handles_none_and_empty_values() -> None:
    assert normalize_symbols(None) == []
    assert normalize_symbols("") == []
    assert normalize_symbols(["", "  "]) == []
