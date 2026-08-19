from pyspark.sql import DataFrame, functions as F


def build_monthly_sales(sales: DataFrame) -> DataFrame:
    return (
        sales.filter(F.col("order_status") == "delivered")
        .groupBy(F.date_trunc("month", "order_purchase_timestamp").alias("month"))
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.count("*").alias("items"),
            F.round(F.sum("price"), 2).alias("product_revenue"),
            F.round(F.sum("freight_value"), 2).alias("freight_revenue"),
            F.round(F.sum("gross_item_value"), 2).alias("gross_revenue"),
        )
    )


def build_category_performance(sales: DataFrame, products: DataFrame) -> DataFrame:
    return (
        sales.join(
            products.select(
                "product_id", "product_category_name", "product_category_name_english"
            ),
            "product_id",
            "left",
        )
        .filter(F.col("order_status") == "delivered")
        .groupBy("product_category_name", "product_category_name_english")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.count("*").alias("items"),
            F.round(F.sum("price"), 2).alias("product_revenue"),
            F.round(F.avg("price"), 2).alias("avg_item_price"),
        )
    )
