from __future__ import annotations

from enum import Enum

from pyspark.sql import DataFrame, SparkSession, functions as F

from src.common.config import Settings, settings
from src.common.schemas import SCHEMAS


class WriteMode(str, Enum):
    OVERWRITE = "overwrite"
    APPEND = "append"


class LakehouseRepository:
    """Read Bronze objects and persist curated DataFrames as Iceberg tables."""

    def __init__(
        self,
        spark: SparkSession,
        config: Settings = settings,
    ) -> None:
        self.spark = spark
        self.config = config

    def read_bronze(self, dataset: str) -> DataFrame:
        schema = SCHEMAS.get(dataset)
        if schema is None:
            supported = ", ".join(sorted(SCHEMAS))
            raise KeyError(f"Unknown dataset '{dataset}'. Supported: {supported}")

        path = f"{self.config.bronze_base}/{dataset}/batch_id=*/*.csv"
        return (
            self.spark.read.option("header", True)
            .option("mode", "PERMISSIVE")
            .schema(schema)
            .csv(path)
            .withColumn("_source_file", F.input_file_name())
            .withColumn(
                "_batch_id",
                F.regexp_extract("_source_file", r"batch_id=([^/]+)", 1),
            )
            .withColumn("_ingested_at", F.current_timestamp())
        )

    def read_silver(self, table_name: str) -> DataFrame:
        return self.spark.table(f"{self.config.silver_catalog_namespace}.{table_name}")

    def ensure_namespace(self, namespace: str) -> None:
        self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    def write_iceberg(
        self,
        dataframe: DataFrame,
        table_name: str,
        mode: WriteMode = WriteMode.OVERWRITE,
    ) -> None:
        writer = dataframe.writeTo(table_name).tableProperty("format-version", "2")
        if mode is WriteMode.OVERWRITE:
            writer.createOrReplace()
        else:
            writer.append()


def read_bronze_csv(
    spark: SparkSession,
    dataset: str,
    cfg: Settings = settings,
) -> DataFrame:
    """Compatibility helper used by notebooks and integration tests."""
    return LakehouseRepository(spark, cfg).read_bronze(dataset)


def write_iceberg_table(df: DataFrame, table: str, mode: str = "overwrite") -> None:
    try:
        write_mode = WriteMode(mode)
    except ValueError as exc:
        raise ValueError("mode must be 'overwrite' or 'append'") from exc

    writer = df.writeTo(table).tableProperty("format-version", "2")
    if write_mode is WriteMode.OVERWRITE:
        writer.createOrReplace()
    else:
        writer.append()
