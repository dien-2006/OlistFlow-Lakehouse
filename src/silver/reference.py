from pyspark.sql import DataFrame, functions as F


def transform_sellers(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("seller_id", F.trim("seller_id"))
        .filter(F.col("seller_id").isNotNull() & (F.col("seller_id") != ""))
        .withColumn("seller_city", F.upper(F.trim("seller_city")))
        .withColumn("seller_state", F.upper(F.trim("seller_state")))
        .dropDuplicates(["seller_id"])
    )


def transform_reviews(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("review_id", F.trim("review_id"))
        .withColumn("order_id", F.trim("order_id"))
        .filter(
            F.col("review_id").isNotNull()
            & (F.col("review_id") != "")
            & F.col("order_id").isNotNull()
            & (F.col("order_id") != "")
        )
        .withColumn("review_creation_date", F.to_timestamp("review_creation_date"))
        .withColumn(
            "review_answer_timestamp", F.to_timestamp("review_answer_timestamp")
        )
        .withColumn(
            "review_score",
            F.when(F.col("review_score").between(1, 5), F.col("review_score")),
        )
        .dropDuplicates(["review_id", "order_id"])
    )


def transform_geolocation(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("geolocation_zip_code_prefix").isNotNull())
        .withColumn("geolocation_city", F.upper(F.trim("geolocation_city")))
        .withColumn("geolocation_state", F.upper(F.trim("geolocation_state")))
        .dropDuplicates()
    )


def transform_category_translation(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("product_category_name", F.lower(F.trim("product_category_name")))
        .withColumn(
            "product_category_name_english",
            F.lower(F.trim("product_category_name_english")),
        )
        .filter(
            F.col("product_category_name").isNotNull()
            & (F.col("product_category_name") != "")
        )
        .dropDuplicates(["product_category_name"])
    )
