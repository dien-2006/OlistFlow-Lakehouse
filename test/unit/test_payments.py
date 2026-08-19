from src.silver.payments import transform_payments


def _columns():
    return [
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    ]


def test_payments_composite_key_dedup(spark):
    rows = [
        ("o1", 1, "CREDIT_CARD", 1, 100.0),
        ("o1", 1, "CREDIT_CARD", 1, 100.0),
        ("o1", 2, "VOUCHER", 1, 20.0),
    ]

    df = spark.createDataFrame(rows, _columns())

    assert transform_payments(df).count() == 2


def test_payments_normalize_and_validate_values(spark):
    df = spark.createDataFrame(
        [(" o1 ", 1, " CREDIT_CARD ", -1, -100.0)],
        _columns(),
    )

    row = transform_payments(df).first()

    assert row["order_id"] == "o1"
    assert row["payment_type"] == "credit_card"
    assert row["payment_installments"] is None
    assert row["payment_value"] is None
