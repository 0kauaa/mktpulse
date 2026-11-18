from __future__ import annotations

from typing import Protocol

from mktpulse.config import Settings

from .brapi import BrapiClient, BrapiError
from .yahoo import YahooClient, YahooError

class QuoteSource(Protocol):
    def fetch_quotes(self, symbols: list[str], *, range_: str = "1y", interval: str = "1d",) -> dict[str, dict]: ...

def create_quote_source(settings: Settings) -> QuoteSource:
    if settings.source == "brapi":
        return BrapiClient(settings.brapi_base_url, token=settings.brapi_token_value, timeout_seconds=settings.request_timeout_seconds, max_retries=settings.max_retries)
    return YahooClient(settings.yahoo_base_url, timeout_seconds=settings.request_timeout_seconds, max_retries=settings.max_retries)

__all__ = ["BrapiClient", "BrapiError", "QuoteSource", "YahooClient", "YahooError", "create_quote_source"]
