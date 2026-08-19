from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path

from src.bronze.ingest_olist import BatchManifest, BronzeIngestionService
from src.common.config import Settings, settings
from src.common.io import LakehouseRepository
from src.common.minio_storage import MinioStorage, ObjectStorage
from src.common.quality import DataQualityValidator
from src.common.spark import SparkSessionFactory
from src.gold.pipeline import GoldPipeline
from src.silver.pipeline import SilverPipeline


class PipelineLayer(str, Enum):
    ALL = "all"
    SILVER = "silver"
    GOLD = "gold"


@dataclass(frozen=True)
class PipelineOptions:
    raw_dir: Path = Path("data/raw")
    batch_id: str | None = None
    skip_ingestion: bool = False
    layer: PipelineLayer = PipelineLayer.ALL


@dataclass(frozen=True)
class PipelineResult:
    manifest: BatchManifest | None = None
    silver_rows: dict[str, int] = field(default_factory=dict)
    gold_rows: dict[str, int] = field(default_factory=dict)


class OlistETLPipeline:
    """Coordinate Bronze, Silver and Gold while owning the Spark lifecycle.

    Transformation rules deliberately live outside this class so that they stay
    deterministic, independently testable and reusable.
    """

    def __init__(
        self,
        config: Settings = settings,
        spark_factory: SparkSessionFactory | None = None,
        storage: ObjectStorage | None = None,
    ) -> None:
        self._config = config
        self._spark_factory = spark_factory or SparkSessionFactory(config)
        self._storage = storage or MinioStorage(config)

    def run(self, options: PipelineOptions) -> PipelineResult:
        manifest = self._run_ingestion(options)
        spark = self._spark_factory.create("olist-batch-etl")

        try:
            repository = LakehouseRepository(spark, self._config)
            return PipelineResult(
                manifest=manifest,
                silver_rows=self._run_silver(options, repository),
                gold_rows=self._run_gold(options, repository),
            )
        finally:
            spark.stop()

    def _run_ingestion(self, options: PipelineOptions) -> BatchManifest | None:
        if options.skip_ingestion:
            return None
        return BronzeIngestionService(self._storage, self._config).ingest(
            options.raw_dir,
            options.batch_id,
        )

    def _run_silver(
        self,
        options: PipelineOptions,
        repository: LakehouseRepository,
    ) -> dict[str, int]:
        if options.layer not in {PipelineLayer.ALL, PipelineLayer.SILVER}:
            return {}
        return SilverPipeline(
            repository,
            DataQualityValidator(),
            self._config,
        ).run()

    def _run_gold(
        self,
        options: PipelineOptions,
        repository: LakehouseRepository,
    ) -> dict[str, int]:
        if options.layer not in {PipelineLayer.ALL, PipelineLayer.GOLD}:
            return {}
        return GoldPipeline(repository, self._config).run()


def parse_args() -> PipelineOptions:
    parser = argparse.ArgumentParser(
        description="Olist batch ETL: local CSV -> MinIO -> Iceberg Silver/Gold"
    )
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--batch-id")
    parser.add_argument("--skip-ingest", action="store_true")
    parser.add_argument(
        "--layer",
        type=PipelineLayer,
        choices=list(PipelineLayer),
        default=PipelineLayer.ALL,
    )
    args = parser.parse_args()
    return PipelineOptions(
        raw_dir=args.raw_dir,
        batch_id=args.batch_id,
        skip_ingestion=args.skip_ingest,
        layer=args.layer,
    )


def main() -> None:
    result = OlistETLPipeline().run(parse_args())
    print(
        "[pipeline:completed] "
        f"silver_tables={len(result.silver_rows)} "
        f"gold_tables={len(result.gold_rows)}"
    )


if __name__ == "__main__":
    main()
