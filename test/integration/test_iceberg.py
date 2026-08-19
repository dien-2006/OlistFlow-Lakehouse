import pytest

from src.common.spark import build_spark_session
from src.common.config import settings


pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def infra_spark():
    spark = build_spark_session("olist-iceberg-integration-test")
    yield spark
    spark.stop()


def test_silver_namespace_accessible(infra_spark):
    rows = infra_spark.sql(
        f"SHOW TABLES IN {settings.silver_catalog_namespace}"
    ).collect()

    assert isinstance(rows, list)


def test_orders_table_if_present_is_queryable(infra_spark):
    table = f"{settings.silver_catalog_namespace}.orders"

    tables = {
        row.tableName
        for row in infra_spark.sql(
            f"SHOW TABLES IN {settings.silver_catalog_namespace}"
        ).collect()
    }

    if "orders" not in tables:
        pytest.skip("Silver orders table has not been created yet")

    df = infra_spark.table(table)

    assert "order_id" in df.columns


def test_orders_is_iceberg_v2_if_table_present(infra_spark):
    table = f"{settings.silver_catalog_namespace}.orders"

    tables = {
        row.tableName
        for row in infra_spark.sql(
            f"SHOW TABLES IN {settings.silver_catalog_namespace}"
        ).collect()
    }

    if "orders" not in tables:
        pytest.skip("Silver orders table has not been created yet")

    properties = {
        row.key: row.value
        for row in infra_spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    }
    assert properties["format-version"] == "2"
