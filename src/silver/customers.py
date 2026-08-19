from pyspark.sql import DataFrame, functions as F


def transform_customers(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("customer_id", F.trim("customer_id"))
        .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
        .withColumn("customer_unique_id", F.trim("customer_unique_id"))
        .withColumn("customer_city", F.upper(F.trim("customer_city")))
        .withColumn("customer_state", F.upper(F.trim("customer_state")))
        .dropDuplicates(["customer_id"])
    )
