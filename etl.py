from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession, DataFrameReader
from dotenv import load_dotenv
from pathlib import Path
import os
import sys


def create_spark_session():
    """Return a SparkSession object."""
    # spark = SparkSession.builder.config(
    #     "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    # ).getOrCreate()
    spark = SparkSession.builder.getOrCreate()
    # TODO clean-up
    print(f"\ncreate_spark_session() --> {type(spark)}\n")
    return spark


def get_command():
    """Gets the command from the user."""

    cmd = ""
    while cmd not in ["L", "A", "Q"]:
        cmd = input("\nL - [L]ocal\nA - [A]WS\nQ - [Q]uit\nCommand: ")[0].upper()
    return cmd


def main():
    """Extracts, transforms and loads the traffic accident data."""

    print("**** Main ****")
    env_path = Path(".") / ".env"
    load_dotenv(dotenv_path=env_path, verbose=True)

    assert (os.getenv("DATA_PATH_LOCAL") is not None) and (
        os.getenv("DATA_PATH_S3") is not None
    ), "DATA_PATH_LOCAL is not your environment and cannot locate data"

    data_path = ""

    while True:
        cmd = get_command()
        if cmd == "L":
            data_path = os.getenv("DATA_PATH_LOCAL")
            city_lat_lon_key = os.getenv("CITY_LAT_LON_KEY")
            fars_key = os.getenv("FARS_KEY")
            print(f"\nRunning locally using data from {data_path}\n")
            break

        elif cmd == "A":
            data_path = os.getenv("DATA_PATH_S3")
            print("\nRunning on AWS using data from {data_path}\n")
            raise NotImplementedError()

        elif cmd == "Q":
            print("\nExiting")
            sys.exit(0)

    spark = create_spark_session()
    all_acc_df = spark.read.csv(
        "../../Data/FARS/CSV/FARS*NationalCSV/ACCIDENT.csv",
        header=True,
        inferSchema=True,
        mode="DROPMALFORMED",
    )
    print(f"accident count: {all_acc_df.count():,}")

    pb_df = spark.read.csv(
        [
            "../../Data/FARS/CSV/FARS2014NationalCSV/PBTYPE.csv",
            "../../Data/FARS/CSV/FARS2015NationalCSV/PBTYPE.csv",
            "../../Data/FARS/CSV/FARS2016NationalCSV/PBTYPE.csv",
            "../../Data/FARS/CSV/FARS2017NationalCSV/PBTYPE.csv",
            "../../Data/FARS/CSV/FARS2018NationalCSV/PBTYPE.csv",
        ],
        header=True,
        inferSchema=True,
        mode="DROPMALFORMED",
    )

    acc_with_pb_df = spark.read.csv(
        [
            "../../Data/FARS/CSV/FARS2014NationalCSV/ACCIDENT.csv",
            "../../Data/FARS/CSV/FARS2015NationalCSV/ACCIDENT.csv",
            "../../Data/FARS/CSV/FARS2016NationalCSV/ACCIDENT.csv",
            "../../Data/FARS/CSV/FARS2017NationalCSV/ACCIDENT.csv",
            "../../Data/FARS/CSV/FARS2018NationalCSV/ACCIDENT.csv",
        ],
        header=True,
        inferSchema=True,
        mode="DROPMALFORMED",
    )
    print(f"pb_df.count() -> {pb_df.count():,}")
    join_expression = acc_with_pb_df["ST_CASE"] == pb_df["ST_CASE"]
    pb_acc_df = pb_df.join(acc_with_pb_df, join_expression, how="left")
    print(f"acc_pb_df.count() -> {pb_acc_df.count():,}")

    # all accidents with consisten coding
    all_acc_aux_df = spark.read.csv(
        "../../Data/FARS/CSV/FARS*NationalCSV/ACC_AUX.csv",
        header=True,
        inferSchema=True,
        mode="DROPMALFORMED",
    )

    # inner join acc_aux and accident dfs
    all_acc_df.createOrReplaceTempView("all_acc_view")
    all_acc_aux_df.createOrReplaceTempView("acc_aux_view")

    # TODO - loop over directories, join files, create a list of dataframes, append them

    acc_df = spark.sql(
        """
        SELECT * FROM all_acc_view all
        INNER JOIN acc_aux_view aux
        ON (all.ST_CASE = aux.ST_CASE)
        AND (all.YEAR = aux.YEAR)"""
    )
    print(f"acc_df records: {acc_df.show(5)}")
    print(f"acc_df count = : {acc_df.count():,}")


if __name__ == "__main__":
    main()
