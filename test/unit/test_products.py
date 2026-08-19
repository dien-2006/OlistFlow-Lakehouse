from src.silver.products import transform_products


def _columns():
    return [
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]


def test_products_rename_typo_columns(spark):
    df = spark.createDataFrame(
        [("p1", " HOUSE ", 10, 20, 1, 100.0, 10.0, 20.0, 30.0)],
        _columns(),
    )

    result = transform_products(df)

    assert "product_name_length" in result.columns
    assert "product_description_length" in result.columns
    assert "product_name_lenght" not in result.columns
    assert "product_description_lenght" not in result.columns


def test_products_normalize_category(spark):
    df = spark.createDataFrame(
        [("p1", " HOUSE ", 10, 20, 1, 100.0, 10.0, 20.0, 30.0)],
        _columns(),
    )

    assert transform_products(df).first()["product_category_name"] == "house"
