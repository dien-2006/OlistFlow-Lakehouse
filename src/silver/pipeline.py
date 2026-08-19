from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame

from src.common.config import Settings, settings
from src.common.io import LakehouseRepository
from src.common.quality import DataQualityValidator
from src.silver.customers import transform_customers
from src.silver.order_items import transform_order_items
from src.silver.orders import transform_orders
from src.silver.payments import transform_payments
from src.silver.products import transform_products
from src.silver.reference import (
    transform_category_translation,
    transform_geolocation,
    transform_reviews,
    transform_sellers,
)

Transformer = Callable[[DataFrame], DataFrame]


@dataclass(frozen=True)
class SilverDataset:
    name: str
    business_keys: tuple[str, ...]
    transformer: Transformer


SILVER_DATASETS = (
    SilverDataset("customers", ("customer_id",), transform_customers),
    SilverDataset("orders", ("order_id", "customer_id"), transform_orders),
    SilverDataset("order_items", ("order_id", "order_item_id"), transform_order_items),
    SilverDataset("payments", ("order_id", "payment_sequential"), transform_payments),
    SilverDataset("products", ("product_id",), transform_products),
    SilverDataset("sellers", ("seller_id",), transform_sellers),
    SilverDataset("reviews", ("review_id", "order_id"), transform_reviews),
    SilverDataset(
        "geolocation", ("geolocation_zip_code_prefix",), transform_geolocation
    ),
    SilverDataset(
        "category_translation",
        ("product_category_name",),
        transform_category_translation,
    ),
)


class SilverPipeline:
    """Convert every registered Bronze dataset into a trusted Iceberg table."""

    def __init__(
        self,
        repository: LakehouseRepository,
        validator: DataQualityValidator,
        config: Settings = settings,
    ) -> None:
        self._repository = repository
        self._validator = validator
        self._config = config

    def run(self) -> dict[str, int]:
        self._repository.ensure_namespace(self._config.silver_catalog_namespace)
        results: dict[str, int] = {}

        for dataset in SILVER_DATASETS:
            results[dataset.name] = self._process(dataset)

        return results

    def _process(self, dataset: SilverDataset) -> int:
        raw = self._repository.read_bronze(dataset.name)
        clean = dataset.transformer(raw).cache()
        try:
            report = self._validator.validate_required_keys(
                clean,
                dataset.business_keys,
                dataset.name,
            )
            table_name = f"{self._config.silver_catalog_namespace}.{dataset.name}"
            self._repository.write_iceberg(clean, table_name)
            print(f"[silver] {dataset.name}: {report.row_count:,} rows")
            return report.row_count
        finally:
            clean.unpersist()


def run_silver(spark) -> dict[str, int]:
    """Compatibility helper for existing notebooks and jobs."""
    repository = LakehouseRepository(spark)
    return SilverPipeline(repository, DataQualityValidator()).run()
