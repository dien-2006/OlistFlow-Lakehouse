import pytest
from pyspark.sql.types import StringType, StructField, StructType

from src.common.quality import (
    duplicate_count,
    required_key_violations,
    assert_no_null_keys,
)


def test_duplicate_count_uses_given_keys(spark):
    df = spark.createDataFrame(
        [("a", 1), ("a", 1), ("a", 2)],
        ["id", "seq"],
    )

    assert duplicate_count(df, ["id", "seq"]) == 1
    assert duplicate_count(df, ["id"]) == 1


def test_required_key_violations(spark):
    df = spark.createDataFrame(
        [("a", 1), (None, 2), ("c", None)],
        ["id", "seq"],
    )

    assert required_key_violations(df, ["id", "seq"]) == 2


def test_assert_no_null_keys_raises(spark):
    df = spark.createDataFrame(
        [(None,)],
        StructType([StructField("id", StringType(), True)]),
    )

    with pytest.raises(ValueError):
        assert_no_null_keys(df, ["id"], "example")
