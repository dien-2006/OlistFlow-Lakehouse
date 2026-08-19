from pyspark.sql import DataFrame, functions as F

TIMESTAMP_COLUMNS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]


def transform_orders(df: DataFrame) -> DataFrame:
    result = (
        df.withColumn("order_id", F.trim("order_id"))
        .withColumn("customer_id", F.trim("customer_id"))
        .filter(
            F.col("order_id").isNotNull()
            & (F.col("order_id") != "")
            & F.col("customer_id").isNotNull()
            & (F.col("customer_id") != "")
        )
        .withColumn("order_status", F.lower(F.trim("order_status")))
    )
    for column in TIMESTAMP_COLUMNS:
        result = result.withColumn(column, F.to_timestamp(column))
    return result.dropDuplicates(["order_id"])
