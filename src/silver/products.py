from pyspark.sql import DataFrame, functions as F


def transform_products(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("product_id", F.trim("product_id"))
        .filter(F.col("product_id").isNotNull() & (F.col("product_id") != ""))
        .withColumn("product_category_name", F.lower(F.trim("product_category_name")))
        .withColumnRenamed("product_name_lenght", "product_name_length")
        .withColumnRenamed("product_description_lenght", "product_description_length")
        .dropDuplicates(["product_id"])
    )
