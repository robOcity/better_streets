from pyspark.sql import (
    SparkSession,
    DataFrame,
)
from pathlib import Path, PosixPath
import pytest
from steer import etl
from steer import utils


@pytest.fixture
def spark():
    return SparkSession.builder.getOrCreate()


@pytest.fixture
def load_env():
    from dotenv import load_dotenv

    env_path = Path(".") / ".env"
    load_dotenv(dotenv_path=env_path, verbose=True)


def test_find_common_set_of_column_names(spark):

    vals = [(0, 1, 2), (3, 4, 5)]
    cols = ["a", "b", "c"]
    df_a = spark.createDataFrame(vals, cols)

    vals = [(10, 11, 12), (13, 14, 15)]
    cols = ["a", "b", "d"]
    df_b = spark.createDataFrame(vals, cols)

    assert etl.find_common_set_of_column_names([df_a, df_b]) == sorted(["a", "b"])


def test_fix_spaces_in_column_names():
    assert etl.fix_spaces_in_column_names(["\tA\n", "   b", "c    "]) == [
        "A",
        "B",
        "C",
    ]


def test_load_glc_codes(load_env):
    glc_df = etl.load_glc_codes()
    print(glc_df.columns)
    # header doesn't count as data
    assert glc_df.count() > 1

    # Note: I added the underscores to the column names
    expected_cols = [
        "Territory",
        "State_Name",
        "State_Code",
        "City_Code",
        "City_Name",
        "County_Name",
        "County_Code",
    ]
    assert all([col in glc_df.columns for col in expected_cols])


def test_build_dir_year_dict(load_env):
    dirs = [
        Path("../data_dir/1234"),
        Path("../data/4321"),
        Path("../data/1a2b"),
    ]
    dir_yr = etl.build_dir_year_dict(dirs)
    assert len(dir_yr.items()) == 2
    _path, _str = dir_yr.popitem()
    assert isinstance(_path, Path)
    assert isinstance(_str, str)


def test_convert_dms_to_dd():
    d, m, s = 20, 30, 30
    assert utils.convert_dms_to_dd(d, m, s) == pytest.approx(20.508333)


def test_extract_city_by_code(spark):

    spark = SparkSession.builder.getOrCreate()

    vals = [
        (-104.990051, 39.686947, "0600", "08"),
        (-104.958094, 39.726886, "0600", "08"),
        (-122.358383, 47.562591, "1960", "053"),
        (-122.289466, 47.626518, "1960", "053"),
        (-119.223081, 34.167788, "2620", "06"),
        (-119.124224, 34.239565, "2620", "06"),
    ]
    cols = ["Latitude", "Longitude", "City_Code", "State_Code"]
    df = spark.createDataFrame(vals, cols)
    assert df.count() == 6
    assert etl.extract_city_by_code(df, "0600", "08").count() == 2


# def test_extract_by_location():

#     spark = SparkSession.builder.getOrCreate()

#     vals = [
#         (-104.990051, 39.686947, "Denver", "Colorado"),
#         (-104.958094, 39.726886, "Denver", "Colorado"),
#         (-122.358383, 47.562591, "Seattle", "Washington"),
#         (-122.289466, 47.626518, "Seattle", "Washington"),
#         (-119.223081, 34.167788, "Oxnard", "California"),
#         (-119.124224, 34.239565, "Oxnard", "California"),
#     ]
#     cols = ["LATITUDE", "LONGITUDE", "CITY", "STATE"]
#     df = spark.createDataFrame(vals, cols)
#     assert df.count() == 6
#     assert etl.extract_by_name(df, "denveR", "COLoRADO").count() == 2

