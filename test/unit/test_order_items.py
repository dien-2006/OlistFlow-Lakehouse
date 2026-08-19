from src.silver.order_items import transform_order_items


def _columns():
    return [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "shipping_limit_date",
        "price",
        "freight_value",
    ]


def test_order_items_use_composite_key_for_dedup(spark):
    rows = [
        ("o1", 1, "p1", "s1", "2018-01-01 10:00:00", 10.0, 2.0),
        ("o1", 1, "p1", "s1", "2018-01-01 10:00:00", 10.0, 2.0),
        ("o1", 2, "p2", "s1", "2018-01-01 10:00:00", 20.0, 3.0),
    ]
    df = spark.createDataFrame(rows, _columns())

    result = transform_order_items(df)

    assert result.count() == 2


def test_order_items_invalid_negative_values_become_null(spark):
    df = spark.createDataFrame(
        [("o1", 1, "p1", "s1", "2018-01-01 10:00:00", -1.0, -2.0)],
        _columns(),
    )

    row = transform_order_items(df).first()

    assert row["price"] is None
    assert row["freight_value"] is None
