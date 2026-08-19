from pyspark.sql.types import TimestampType
from src.silver.orders import transform_orders
from src.common.schemas import SCHEMAS


def _columns():
    return [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]


def test_orders_filter_required_keys(spark):
    df = spark.createDataFrame(
        [
            ("o1", "c1", "DELIVERED", "2018-01-01 10:00:00", None, None, None, None),
            (None, "c2", "DELIVERED", "2018-01-01 10:00:00", None, None, None, None),
            ("o3", None, "DELIVERED", "2018-01-01 10:00:00", None, None, None, None),
        ],
        SCHEMAS["orders"],
    )

    result = transform_orders(df)

    assert result.count() == 1


def test_orders_normalize_status_and_parse_timestamp(spark):
    df = spark.createDataFrame(
        [
            (
                " o1 ",
                " c1 ",
                " DELIVERED ",
                "2018-01-01 10:00:00",
                None,
                None,
                None,
                None,
            )
        ],
        SCHEMAS["orders"],
    )

    result = transform_orders(df)
    row = result.first()

    assert row["order_id"] == "o1"
    assert row["customer_id"] == "c1"
    assert row["order_status"] == "delivered"
    assert isinstance(result.schema["order_purchase_timestamp"].dataType, TimestampType)


def test_orders_deduplicate_order_id(spark):
    row = ("o1", "c1", "delivered", "2018-01-01 10:00:00", None, None, None, None)
    df = spark.createDataFrame([row, row], SCHEMAS["orders"])

    assert transform_orders(df).count() == 1
