"""Configuracao central do projeto.

Tudo que muda entre ambientes entra por variavel de ambiente (prefixo MKT_).
Segredos nunca ficam no codigo nem no .env versionado.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MKT_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    source: Literal["brapi", "yahoo"] = "yahoo"
    lake_backend: Literal["local", "s3"] = "local"
    lake_root: Path = Path("data/lake")

    s3_bucket: str = ""
    s3_prefix: str = ""
    aws_region: str = "us-east-1"
    s3_endpoint_url: str | None = None

    brapi_base_url: str = "https://brapi.dev/api"
    brapi_token: SecretStr | None = None
    brapi_batch_size: int = Field(default=3, ge=1, le=20)
    yahoo_base_url: str = "https://query2.finance.yahoo.com"
    request_pause_seconds: float = Field(default=0.2, ge=0)
    request_timeout_seconds: float = Field(default=30.0, gt=0)
    max_retries: int = Field(default=5, ge=0)

    symbols: str = "PETR4,VALE3,ITUB4,BBDC4,BBAS3,ABEV3,WEGE3,MGLU3"
    log_level: str = "INFO"

    @field_validator("symbols")
    @classmethod
    def _normalize_symbols(cls, value: str) -> str:
        symbols = [item.strip().upper() for item in value.split(",") if item.strip()]
        if not symbols:
            raise ValueError("MKT_SYMBOLS deve conter pelo menos um ticker")
        return ",".join(symbols)

    @property
    def symbol_list(self) -> list[str]:
        return self.symbols.split(",")

    @property
    def brapi_token_value(self) -> str | None:
        return self.brapi_token.get_secret_value() if self.brapi_token else None


@lru_cache
def get_settings() -> Settings:
    return Settings()
