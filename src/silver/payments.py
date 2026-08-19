from pyspark.sql import DataFrame, functions as F


def transform_payments(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("order_id", F.trim("order_id"))
        .filter(
            F.col("order_id").isNotNull()
            & (F.col("order_id") != "")
            & F.col("payment_sequential").isNotNull()
        )
        .withColumn("payment_type", F.lower(F.trim("payment_type")))
        .withColumn(
            "payment_installments",
            F.when(F.col("payment_installments") >= 0, F.col("payment_installments")),
        )
        .withColumn(
            "payment_value", F.when(F.col("payment_value") >= 0, F.col("payment_value"))
        )
        .dropDuplicates(["order_id", "payment_sequential"])
    )
