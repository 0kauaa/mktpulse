from __future__ import annotations

from pathlib import PurePosixPath
from typing import Protocol, runtime_checkable


def validate_key(key: str) -> str:
    """Garante que a chave e relativa e nao escapa da raiz do lake."""
    if not key or key.startswith("/") or "\\" in key:
        raise ValueError(f"chave de storage invalida: {key!r}")
    parts = PurePosixPath(key).parts
    if any(part in ("", ".", "..") for part in parts):
        raise ValueError(f"chave de storage invalida: {key!r}")
    return key


@runtime_checkable
class Storage(Protocol):
    """Contrato minimo de armazenamento de objetos.

    Local e S3 implementam a mesma interface para que o pipeline nao saiba
    onde o dado esta fisicamente.
    """

    def write_bytes(self, key: str, data: bytes) -> None: ...

    def read_bytes(self, key: str) -> bytes: ...

    def exists(self, key: str) -> bool: ...

    def list(self, prefix: str) -> list[str]: ...
