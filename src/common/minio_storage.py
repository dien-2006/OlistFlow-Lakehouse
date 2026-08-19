from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Protocol

from minio import Minio
from minio.error import S3Error

from src.common.config import Settings, settings


class ObjectStorage(Protocol):
    """Port implemented by object-storage adapters and test doubles."""

    def ensure_bucket(self, bucket: str | None = None) -> None: ...

    def upload(
        self,
        local_path: Path,
        object_name: str,
        content_type: str = "application/octet-stream",
    ) -> None: ...

    def upload_bytes(
        self,
        data: bytes,
        object_name: str,
        content_type: str,
    ) -> None: ...

    def exists(self, object_name: str) -> bool: ...


class MinioStorage:
    """Small adapter around MinIO so infrastructure details stay isolated."""

    def __init__(self, config: Settings = settings) -> None:
        self.config = config
        endpoint = config.minio_endpoint.removeprefix("http://").removeprefix(
            "https://"
        )
        self.client = Minio(
            endpoint,
            access_key=config.minio_access_key,
            secret_key=config.minio_secret_key,
            secure=config.minio_endpoint.startswith("https://"),
        )

    def ensure_bucket(self, bucket: str | None = None) -> None:
        bucket = bucket or self.config.minio_bucket
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)

    def upload(
        self,
        local_path: Path,
        object_name: str,
        content_type: str = "application/octet-stream",
    ) -> None:
        if not local_path.is_file():
            raise FileNotFoundError(local_path)
        self.client.fput_object(
            self.config.minio_bucket,
            object_name,
            str(local_path),
            content_type=content_type,
        )

    def upload_bytes(
        self,
        data: bytes,
        object_name: str,
        content_type: str,
    ) -> None:
        stream = BytesIO(data)
        self.client.put_object(
            self.config.minio_bucket,
            object_name,
            stream,
            length=len(data),
            content_type=content_type,
        )

    def exists(self, object_name: str) -> bool:
        try:
            self.client.stat_object(self.config.minio_bucket, object_name)
            return True
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchObject"}:
                return False
            raise
