from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from pyspark.sql import DataFrame, functions as F


@dataclass(frozen=True)
class QualityReport:
    dataset: str
    row_count: int
    required_key_violations: int


class DataQualityValidator:
    """Run fail-fast quality gates before data reaches a trusted layer."""

    def validate_required_keys(
        self,
        dataframe: DataFrame,
        keys: Iterable[str],
        dataset_name: str,
    ) -> QualityReport:
        keys = tuple(keys)
        missing_columns = sorted(set(keys) - set(dataframe.columns))
        if missing_columns:
            raise ValueError(
                f"{dataset_name}: missing required columns {missing_columns}"
            )

        violations = required_key_violations(dataframe, keys)
        if violations:
            raise ValueError(
                f"{dataset_name}: {violations} row(s) have empty required keys"
            )

        return QualityReport(
            dataset=dataset_name,
            row_count=dataframe.count(),
            required_key_violations=0,
        )


def null_summary(df: DataFrame) -> DataFrame:
    return df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])


def duplicate_count(df: DataFrame, keys: Iterable[str]) -> int:
    return df.groupBy(*list(keys)).count().filter(F.col("count") > 1).count()


def required_key_violations(df: DataFrame, keys: Iterable[str]) -> int:
    condition = F.lit(False)
    for key in keys:
        condition = (
            condition | F.col(key).isNull() | (F.trim(F.col(key).cast("string")) == "")
        )
    return df.filter(condition).count()


def assert_no_null_keys(df: DataFrame, keys: Iterable[str], dataset_name: str) -> None:
    DataQualityValidator().validate_required_keys(df, keys, dataset_name)
