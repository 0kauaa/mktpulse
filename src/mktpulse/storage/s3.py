from __future__ import annotations

from .base import validate_key


class S3Storage:
    """Storage de objetos em S3 (ou MinIO/LocalStack via endpoint_url).

    O boto3 e importado apenas quando este backend e usado, para que o modo
    local nao exija credenciais ou a dependencia extra.
    """

    def __init__(
        self,
        bucket: str,
        *,
        prefix: str = "",
        region: str = "us-east-1",
        endpoint_url: str | None = None,
    ) -> None:
        if not bucket:
            raise ValueError("MKT_S3_BUCKET e obrigatorio quando MKT_LAKE_BACKEND=s3")
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover - depende do ambiente
            raise RuntimeError('instale o extra de S3: pip install -e ".[s3]"') from exc

        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self._client = boto3.client("s3", region_name=region, endpoint_url=endpoint_url)

    def _key(self, key: str) -> str:
        validate_key(key)
        return f"{self.prefix}/{key}" if self.prefix else key

    def _base_prefix(self) -> str:
        return f"{self.prefix}/" if self.prefix else ""

    def write_bytes(self, key: str, data: bytes) -> None:
        self._client.put_object(Bucket=self.bucket, Key=self._key(key), Body=data)

    def read_bytes(self, key: str) -> bytes:
        response = self._client.get_object(Bucket=self.bucket, Key=self._key(key))
        return response["Body"].read()

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self.bucket, Key=self._key(key))
            return True
        except Exception as exc:  # noqa: BLE001 - boto3 tem varias classes de erro
            code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise

    def list(self, prefix: str) -> list[str]:
        if prefix:
            validate_key(prefix)
        base = self._base_prefix()
        paginator = self._client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=f"{base}{prefix}"):
            for obj in page.get("Contents", []):
                full_key = obj["Key"]
                keys.append(full_key[len(base) :] if base else full_key)
        return sorted(keys)
