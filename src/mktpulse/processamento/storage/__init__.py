from __future__ import annotations

from mktpulse.comum.config import Settings

from .base import Storage
from .local import LocalStorage
from .s3 import S3Storage

__all__ = ["LocalStorage", "S3Storage", "Storage", "create_storage"]


def create_storage(settings: Settings) -> Storage:
    if settings.lake_backend == "local":
        return LocalStorage(settings.lake_root)
    return S3Storage(
        settings.s3_bucket,
        prefix=settings.s3_prefix,
        region=settings.aws_region,
        endpoint_url=settings.s3_endpoint_url,
    )
