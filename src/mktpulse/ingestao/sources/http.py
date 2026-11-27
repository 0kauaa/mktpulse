"""utilitarios http compartilhados pelos clientes de fonte."""

from __future__ import annotations

from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

_RETRY_STATUS_CODES = (429, 500, 502, 503, 504)

def build_retrying_session(max_retries: int) -> requests.Session:
    # retry apenas em erros transitorios. `backoff_factor=1.0` dobra a espera
    # entre tentativas; `respect_retry_after_header` obedece o 429 do servidor.
    retry = Retry(
        total=max_retries,
        connect=max_retries,
        read=max_retries,
        status=max_retries,
        backoff_factor=1.0,
        status_forcelist=_RETRY_STATUS_CODES,
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        # deixa a resposta de erro chegar ao cliente, que traduz para erro de dominio.
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    # a mesma politica vale para http e https.
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

def decode_json_body(response: requests.Response) -> Any | None:
    # nem todo erro http tem corpo json; devolver none deixa o cliente decidir.
    try:
        return response.json()
    except ValueError:
        return None
