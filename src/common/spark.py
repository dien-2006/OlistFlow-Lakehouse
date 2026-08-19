from __future__ import annotations

from pyspark.sql import SparkSession

from src.common.config import Settings, settings


class SparkSessionFactory:
    """Build Spark sessions with one consistent lakehouse configuration."""

    def __init__(self, config: Settings = settings) -> None:
        self._config = config

    def create(self, app_name: str) -> SparkSession:
        config = self._config
        catalog = config.iceberg_catalog

        return (
            SparkSession.builder.appName(app_name)
            .master(config.spark_master)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "16")
            .config("spark.hadoop.fs.s3a.endpoint", config.minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", config.minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", config.minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config(
                f"spark.sql.catalog.{catalog}",
                "org.apache.iceberg.spark.SparkCatalog",
            )
            .config(f"spark.sql.catalog.{catalog}.type", "rest")
            .config(f"spark.sql.catalog.{catalog}.uri", config.iceberg_catalog_uri)
            .config(f"spark.sql.catalog.{catalog}.warehouse", config.iceberg_warehouse)
            .config(
                f"spark.sql.catalog.{catalog}.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO",
            )
            .config(
                f"spark.sql.catalog.{catalog}.s3.endpoint", config.iceberg_s3_endpoint
            )
            .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
            .config(
                f"spark.sql.catalog.{catalog}.s3.access-key-id",
                config.minio_access_key,
            )
            .config(
                f"spark.sql.catalog.{catalog}.s3.secret-access-key",
                config.minio_secret_key,
            )
            .getOrCreate()
        )


def build_spark_session(
    app_name: str,
    cfg: Settings = settings,
) -> SparkSession:
    """Compatibility helper for notebooks and tests."""
    return SparkSessionFactory(cfg).create(app_name)
