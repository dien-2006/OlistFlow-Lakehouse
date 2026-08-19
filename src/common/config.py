from __future__ import annotations
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    minio_endpoint: str = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    minio_access_key: str = os.getenv("MINIO_ACCESS_KEY", "admin")
    minio_secret_key: str = os.getenv("MINIO_SECRET_KEY", "password")
    minio_bucket: str = os.getenv("MINIO_BUCKET", "olist-lake")
    spark_master: str = os.getenv("SPARK_MASTER", "local[*]")
    iceberg_catalog: str = os.getenv("ICEBERG_CATALOG", "lakehouse")
    iceberg_catalog_uri: str = os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181")
    iceberg_warehouse: str = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/")
    iceberg_s3_endpoint: str = os.getenv("ICEBERG_S3_ENDPOINT", "http://localhost:9000")
    bronze_prefix: str = os.getenv("BRONZE_PREFIX", "bronze/olist")
    silver_namespace: str = os.getenv("SILVER_NAMESPACE", "olist_silver")
    gold_namespace: str = os.getenv("GOLD_NAMESPACE", "olist_gold")

    @property
    def bronze_base(self) -> str:
        return f"s3a://{self.minio_bucket}/{self.bronze_prefix}"

    @property
    def silver_catalog_namespace(self) -> str:
        return f"{self.iceberg_catalog}.{self.silver_namespace}"

    @property
    def gold_catalog_namespace(self) -> str:
        return f"{self.iceberg_catalog}.{self.gold_namespace}"


settings = Settings()
