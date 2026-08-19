from pyspark.sql import DataFrame, functions as F


def build_fact_sales(orders: DataFrame, items: DataFrame) -> DataFrame:
    return (
        items.alias("i")
        .join(orders.alias("o"), F.col("i.order_id") == F.col("o.order_id"), "inner")
        .select(
            F.col("i.order_id"),
            "i.order_item_id",
            "o.customer_id",
            "i.product_id",
            "i.seller_id",
            "o.order_status",
            "o.order_purchase_timestamp",
            F.to_date("o.order_purchase_timestamp").alias("order_date"),
            "i.price",
            "i.freight_value",
            (
                F.coalesce(F.col("i.price"), F.lit(0.0))
                + F.coalesce(F.col("i.freight_value"), F.lit(0.0))
            ).alias("gross_item_value"),
        )
    )


def build_fact_payments(payments: DataFrame, orders: DataFrame) -> DataFrame:
    return (
        payments.alias("p")
        .join(orders.alias("o"), "order_id", "inner")
        .select(
            "order_id",
            "p.payment_sequential",
            "o.customer_id",
            "o.order_purchase_timestamp",
            "p.payment_type",
            "p.payment_installments",
            "p.payment_value",
        )
    )
