from pyspark.sql import (
    SparkSession,
    DataFrame,
)
import pytest
import etl


def test_find_common_set_of_column_names():

    spark = SparkSession.builder.getOrCreate()

    vals = [(0, 1, 2), (3, 4, 5)]
    cols = ["a", "b", "c"]
    df_a = spark.createDataFrame(vals, cols)

    vals = [(10, 11, 12), (13, 14, 15)]
    cols = ["a", "b", "d"]
    df_b = spark.createDataFrame(vals, cols)

    assert etl.find_common_set_of_column_names([df_a, df_b]) == sorted(
        ["a", "b"]
    )


def test_fix_spaces_in_column_names():
    assert etl.fix_spaces_in_column_names(["\tA\n", "   b", "c    "]) == [
        "A",
        "B",
        "C",
    ]
