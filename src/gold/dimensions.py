from pyspark.sql import DataFrame, functions as F


def build_dim_customer(customers: DataFrame) -> DataFrame:
    return customers.select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ).dropDuplicates(["customer_id"])


def build_dim_product(products: DataFrame, translations: DataFrame) -> DataFrame:
    return (
        products.alias("p")
        .join(translations.alias("t"), "product_category_name", "left")
        .select(
            "product_id",
            "product_category_name",
            "product_category_name_english",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        )
        .dropDuplicates(["product_id"])
    )


def build_dim_date(orders: DataFrame) -> DataFrame:
    return (
        orders.select(F.to_date("order_purchase_timestamp").alias("date"))
        .filter(F.col("date").isNotNull())
        .distinct()
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
    )
