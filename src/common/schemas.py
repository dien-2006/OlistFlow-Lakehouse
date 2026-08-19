from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def _schema(*fields):
    return StructType([StructField(name, dtype, True) for name, dtype in fields])


SCHEMAS = {
    "customers": _schema(
        ("customer_id", StringType()),
        ("customer_unique_id", StringType()),
        ("customer_zip_code_prefix", IntegerType()),
        ("customer_city", StringType()),
        ("customer_state", StringType()),
    ),
    "orders": _schema(
        ("order_id", StringType()),
        ("customer_id", StringType()),
        ("order_status", StringType()),
        ("order_purchase_timestamp", StringType()),
        ("order_approved_at", StringType()),
        ("order_delivered_carrier_date", StringType()),
        ("order_delivered_customer_date", StringType()),
        ("order_estimated_delivery_date", StringType()),
    ),
    "order_items": _schema(
        ("order_id", StringType()),
        ("order_item_id", IntegerType()),
        ("product_id", StringType()),
        ("seller_id", StringType()),
        ("shipping_limit_date", StringType()),
        ("price", DoubleType()),
        ("freight_value", DoubleType()),
    ),
    "payments": _schema(
        ("order_id", StringType()),
        ("payment_sequential", IntegerType()),
        ("payment_type", StringType()),
        ("payment_installments", IntegerType()),
        ("payment_value", DoubleType()),
    ),
    "products": _schema(
        ("product_id", StringType()),
        ("product_category_name", StringType()),
        ("product_name_lenght", IntegerType()),
        ("product_description_lenght", IntegerType()),
        ("product_photos_qty", IntegerType()),
        ("product_weight_g", DoubleType()),
        ("product_length_cm", DoubleType()),
        ("product_height_cm", DoubleType()),
        ("product_width_cm", DoubleType()),
    ),
    "sellers": _schema(
        ("seller_id", StringType()),
        ("seller_zip_code_prefix", IntegerType()),
        ("seller_city", StringType()),
        ("seller_state", StringType()),
    ),
    "reviews": _schema(
        ("review_id", StringType()),
        ("order_id", StringType()),
        ("review_score", IntegerType()),
        ("review_comment_title", StringType()),
        ("review_comment_message", StringType()),
        ("review_creation_date", StringType()),
        ("review_answer_timestamp", StringType()),
    ),
    "geolocation": _schema(
        ("geolocation_zip_code_prefix", IntegerType()),
        ("geolocation_lat", DoubleType()),
        ("geolocation_lng", DoubleType()),
        ("geolocation_city", StringType()),
        ("geolocation_state", StringType()),
    ),
    "category_translation": _schema(
        ("product_category_name", StringType()),
        ("product_category_name_english", StringType()),
    ),
}
