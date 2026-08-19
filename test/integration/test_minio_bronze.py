import pytest

from src.common.spark import build_spark_session
from src.common.io import read_bronze_csv


pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def infra_spark():
    spark = build_spark_session("olist-minio-integration-test")
    yield spark
    spark.stop()


def test_bronze_customers_is_readable_from_minio(infra_spark):
    df = read_bronze_csv(infra_spark, "customers")

    assert df.columns
    assert df.limit(1).count() <= 1


def test_bronze_orders_has_expected_columns(infra_spark):
    df = read_bronze_csv(infra_spark, "orders")

    assert "order_id" in df.columns
    assert "customer_id" in df.columns
    assert "order_purchase_timestamp" in df.columns
