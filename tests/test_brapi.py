from __future__ import annotations

from typing import Any

import pytest

from mktpulse.sources.brapi import BrapiClient, BrapiError
from tests.conftest import make_result

class FakeResponse:
    def __init__(self, status_code: int, payload: Any) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> Any:
        return self._payload

class FakeSession:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, **kwargs: Any) -> FakeResponse:
        self.calls.append({"url": url, **kwargs})
        return self.response

def test_fetch_quotes_builds_request_without_leaking_token() -> None:
    session = FakeSession(FakeResponse(200, {"results": [make_result()]}))
    client = BrapiClient(token="super-secret", session=session)

    quotes = client.fetch_quotes(["petr4"], range_="3mo", interval="1d")

    assert quotes["PETR4"]["symbol"] == "PETR4"
    call = session.calls[0]
    assert call["url"].endswith("/quote/PETR4")
    assert call["params"] == {"range": "3mo", "interval": "1d", "token": "super-secret"}
    assert "super-secret" not in call["url"]
    assert call["timeout"] == 30.0

def test_http_error_is_mapped_without_token_in_message() -> None:
    session = FakeSession(
        FakeResponse(
            401,
            {
                "error": True,
                "message": "Token de autenticacao nao fornecido",
                "code": "MISSING_TOKEN",
            },
        )
    )
    client = BrapiClient(token="super-secret", session=session)

    with pytest.raises(BrapiError) as error:
        client.fetch_quotes(["PETR4"])

    assert error.value.status_code == 401
    assert error.value.code == "MISSING_TOKEN"
    assert "super-secret" not in str(error.value)

def test_invalid_shape_is_rejected() -> None:
    session = FakeSession(FakeResponse(200, {"results": "not-a-list"}))
    client = BrapiClient(session=session)

    with pytest.raises(BrapiError, match="results"):
        client.fetch_quotes(["PETR4"])

def test_empty_symbols_is_rejected() -> None:
    client = BrapiClient(session=FakeSession(FakeResponse(200, {"results": []})))

    with pytest.raises(ValueError):
        client.fetch_quotes([" "])
