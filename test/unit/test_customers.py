from src.silver.customers import transform_customers


def test_customers_remove_null_customer_id(spark):
    df = spark.createDataFrame(
        [
            ("c1", "u1", 1000, " sao paulo ", "sp"),
            (None, "u2", 2000, "rio", "rj"),
        ],
        [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
    )

    result = transform_customers(df)

    assert result.count() == 1
    assert result.first()["customer_id"] == "c1"


def test_customers_deduplicate_by_customer_id(spark):
    df = spark.createDataFrame(
        [
            ("c1", "u1", 1000, "A", "SP"),
            ("c1", "u1", 1000, "A", "SP"),
        ],
        [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
    )

    result = transform_customers(df)

    assert result.count() == 1


def test_customers_normalize_strings(spark):
    df = spark.createDataFrame(
        [(" c1 ", " u1 ", 1000, " sao paulo ", " sp ")],
        [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
    )

    row = transform_customers(df).first()

    assert row["customer_id"] == "c1"
    assert row["customer_unique_id"] == "u1"
    assert row["customer_city"] == "SAO PAULO"
    assert row["customer_state"] == "SP"
