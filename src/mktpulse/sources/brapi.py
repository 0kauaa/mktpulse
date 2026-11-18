"""
define o cliente da brapi.
duas classes: BrapiError e BrapiClient

pontos operacionais:
- retry com backoff exponencial para 429 e 5xx;
- timeout explicito (nunca deixar request pendurada);
- token via query string, mas nunca em log ou mensagem de erro;
- o plano gratuito limita o lote a 3 tickers, entao o pipeline fatia a lista.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_USER_AGENT = "mktpulse/0.1 (data engineering study project)"

class BrapiError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, code: str | None = None,) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code

class BrapiClient:
    def __init__(self, base_url: str = "https://brapi.dev/api", *,
                 token: str | None = None, timeout_seconds: float = 30.0,
                 max_retries: int = 5, session: requests.Session | None = None) -> None:

        self._base_url = base_url.rstrip("/")
        self._token = token
        self._timeout = timeout_seconds
        self._session = session or self._build_session(max_retries)

    @staticmethod
    def _build_session(max_retries: int) -> requests.Session:
        retry = Retry(total=max_retries,
                      connect=max_retries,
                      read=max_retries,
                      status=max_retries,
                      backoff_factor=1.0,
                      status_forcelist=(429, 500, 502, 503, 504),
                      allowed_methods=frozenset({"GET"}),
                      respect_retry_after_header=True,
                      raise_on_status=False)

        adapter = HTTPAdapter(max_retries=retry)
        session = requests.Session()
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def fetch_quotes(self, symbols: Sequence[str], *, range_: str = "1y", interval: str = "1d") -> dict[str, dict[str, Any]]:
        """Retorna {ticker: payload bruto do ticker}."""

        tickers = [symbol.strip().upper() for symbol in symbols if symbol.strip()]
        if not tickers:
            raise ValueError("informe pelo menos um ticker")

        url = f"{self._base_url}/quote/{','.join(tickers)}"
        params: dict[str, str] = {"range": range_, "interval": interval}
        if self._token:
            params["token"] = self._token

        response = self._session.get(url, params=params, timeout=self._timeout, headers={"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/json"})

        data: dict[str, Any] = {}
        if response.status_code >= 400:
            try:
                body = response.json()
                data = body if isinstance(body, dict) else {}
            except ValueError:
                pass
            code = data.get("code")
            message = data.get("message") or "erro sem detalhe"
            hint = ""
            if code == "MISSING_TOKEN":
                hint = " (reduza o lote ou configure MKT_BRAPI_TOKEN)"
            raise BrapiError(f"brapi falhou com HTTP {response.status_code}: {message}{hint}", status_code=response.status_code, code=code)

        try:
            body = response.json()
        except ValueError as exc:
            raise BrapiError("brapi retornou um corpo que nao e JSON") from exc
        if not isinstance(body, dict):
            raise BrapiError("brapi retornou um JSON inesperado")

        if body.get("error"):
            raise BrapiError(f"brapi retornou erro: {body.get('message', 'sem mensagem')}", code=body.get("code"))
        if not isinstance(body.get("results"), list):
            raise BrapiError("resposta da brapi sem a lista 'results'")
        return {
            str(item["symbol"]).upper(): item
            for item in body["results"]
            if isinstance(item, dict) and item.get("symbol")
        }
