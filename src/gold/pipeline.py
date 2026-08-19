from __future__ import annotations

from pyspark.sql import DataFrame

from src.common.config import Settings, settings
from src.common.io import LakehouseRepository
from src.gold.dimensions import build_dim_customer, build_dim_date, build_dim_product
from src.gold.facts import build_fact_payments, build_fact_sales
from src.gold.marts import build_category_performance, build_monthly_sales


class GoldPipeline:
    """Build analytics-ready dimensions, facts and marts from Silver tables."""

    def __init__(
        self,
        repository: LakehouseRepository,
        config: Settings = settings,
    ) -> None:
        self._repository = repository
        self._config = config

    def run(self) -> dict[str, int]:
        self._repository.ensure_namespace(self._config.gold_catalog_namespace)
        outputs = self._build_outputs()
        results: dict[str, int] = {}

        for table_name, dataframe in outputs.items():
            full_name = f"{self._config.gold_catalog_namespace}.{table_name}"
            self._repository.write_iceberg(dataframe, full_name)
            row_count = dataframe.count()
            results[table_name] = row_count
            print(f"[gold] {table_name}: {row_count:,} rows")

        return results

    def _build_outputs(self) -> dict[str, DataFrame]:
        customers = self._repository.read_silver("customers")
        orders = self._repository.read_silver("orders")
        order_items = self._repository.read_silver("order_items")
        payments = self._repository.read_silver("payments")
        products = self._repository.read_silver("products")
        translations = self._repository.read_silver("category_translation")

        dim_product = build_dim_product(products, translations)
        fact_sales = build_fact_sales(orders, order_items)

        return {
            "dim_customer": build_dim_customer(customers),
            "dim_product": dim_product,
            "dim_date": build_dim_date(orders),
            "fact_sales": fact_sales,
            "fact_payments": build_fact_payments(payments, orders),
            "monthly_sales": build_monthly_sales(fact_sales),
            "category_performance": build_category_performance(fact_sales, dim_product),
        }


def run_gold(spark) -> dict[str, int]:
    """Compatibility helper for existing notebooks and jobs."""
    return GoldPipeline(LakehouseRepository(spark)).run()
