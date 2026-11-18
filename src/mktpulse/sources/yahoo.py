"""
define o cliente do endpoint de chart do yahoo finance.
duas classes: YahooError e YahooClient

usado como fonte padrao por nao exigir token e permitir reproducao do projeto.
o endpoint e nao oficial: por isso timeout, retry e validacao de contrato sao
obrigatorios. o cliente busca um ticker por request e devolve o payload bruto
de cada um para o landing zone.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# UA curto e aceito pelo endpoint; UAs de browser completos tem sido
# bloqueados com 429. Nao e uma API oficial: mantenha isso configuravel.
DEFAULT_USER_AGENT = "Mozilla/5.0"


class YahooError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code

class YahooClient:
    def __init__(self, base_url: str = "https://query2.finance.yahoo.com", *,
                 timeout_seconds: float = 30.0, max_retries: int = 5,
                 session: requests.Session | None = None,) -> None:

        self._base_url = base_url.rstrip("/")
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

    def fetch_quotes(self, symbols: Sequence[str], *,range_: str = "1y", interval: str = "1d",) -> dict[str, dict[str, Any]]:
        """Retorna {ticker solicitado: payload bruto do chart.result[0]}."""

        tickers = [symbol.strip().upper() for symbol in symbols if symbol.strip()]
        if not tickers:
            raise ValueError("informe pelo menos um ticker")

        output: dict[str, dict[str, Any]] = {}
        for ticker in tickers:
            yahoo_symbol = ticker
            if "." not in ticker and not ticker.startswith("^"):
                yahoo_symbol = f"{ticker}.SA"

            url = f"{self._base_url}/v8/finance/chart/{yahoo_symbol}"
            response = self._session.get(url,
                                         params={"range": range_,
                                                 "interval": interval,
                                                 "events": "div,splits",
                                                 "includePrePost": "false",
                                                },
                                        timeout=self._timeout,
                                        headers={"User-Agent": DEFAULT_USER_AGENT, "Accept": "application/json"})

            body: dict[str, Any] = {}
            if response.status_code >= 400:
                try:
                    parsed = response.json()
                    body = parsed if isinstance(parsed, dict) else {}
                except ValueError:
                    pass
                chart_error = (body.get("chart") or {}).get("error") or {}
                detail = chart_error.get("description") if isinstance(chart_error, dict) else None
                raise YahooError(f"yahoo falhou com HTTP {response.status_code}: {detail or 'sem detalhe'}", status_code=response.status_code)

            try:
                parsed = response.json()
            except ValueError as exc:
                raise YahooError("yahoo retornou um corpo que nao e JSON") from exc
            if not isinstance(parsed, dict):
                raise YahooError("yahoo retornou um JSON inesperado")

            chart = parsed.get("chart") or {}
            if chart.get("error"):
                error = chart["error"] or {}
                raise YahooError(f"yahoo retornou erro: {error.get('description', error)}")
            results = chart.get("result") or []
            if not results:
                raise YahooError(f"yahoo sem resultado para {ticker}")
            output[ticker] = results[0]

        return output
