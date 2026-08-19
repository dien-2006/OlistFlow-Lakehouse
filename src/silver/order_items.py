from pyspark.sql import DataFrame, functions as F


def transform_order_items(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("order_id", F.trim("order_id"))
        .filter(
            F.col("order_id").isNotNull()
            & (F.col("order_id") != "")
            & F.col("order_item_id").isNotNull()
        )
        .withColumn("product_id", F.trim("product_id"))
        .withColumn("seller_id", F.trim("seller_id"))
        .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
        .withColumn("price", F.when(F.col("price") >= 0, F.col("price")))
        .withColumn(
            "freight_value", F.when(F.col("freight_value") >= 0, F.col("freight_value"))
        )
        .dropDuplicates(["order_id", "order_item_id"])
    )
