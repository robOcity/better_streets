from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession, DataFrameReader, DataFrame
from dotenv import load_dotenv
from pathlib import Path
from functools import reduce
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
        cmd = input("\nL - [L]ocal\nA - [A]WS\nQ - [Q]uit\nCommand: ")[
            0
        ].upper()
    return cmd


def make_filenames_case_consistent(data_path):
    return [
        p.rename(p.parent / p.name.upper())
        for p in Path(data_path).rglob("*.csv")
    ]


def find_dirs_with_both_files(file_1, file_2, data_path):
    return sorted(
        [
            _dir
            for _dir in Path(data_path).iterdir()
            if _dir.joinpath(file_1).exists() and _dir.joinpath(file_2).exists()
        ]
    )


def get_local_paths(root, pattern):
    """Returns a generator of files matching the glob_pattern under the root directory ."""

    path = Path(root)
    return path.glob(pattern)


def read_csv(csv_full_path):
    return create_spark_session().read.csv(
        csv_full_path, header=True, inferSchema=True, mode="DROPMALFORMED",
    )


def get_S3_paths(bucket, pattern):
    raise NotImplementedError("get_S3_paths")


def main():
    """Extracts, transforms and loads the traffic accident data."""

    print("**** Main ****")
    env_path = Path(".") / ".env"
    load_dotenv(dotenv_path=env_path, verbose=True)

    assert (os.getenv("DATA_LOCAL_ROOT") is not None) and (
        os.getenv("DATA_S3_BUCKET") is not None
    ), "Environment variable with the root data directory has not been set"

    data_path = ""

    while True:
        cmd = get_command()
        if cmd == "L":
            data_path = Path(os.getenv("DATA_LOCAL_ROOT")) / os.getenv(
                "FARS_KEY"
            )
            print(f"data_path: {data_path}")
            city_lat_lon_key = os.getenv("CITY_LAT_LON_KEY")
            fars_key = os.getenv("FARS_KEY")
            make_filenames_case_consistent
            print(f"\nRunning locally using data from {data_path}\n")
            break

        elif cmd == "A":
            data_path = Path(os.getenv("DATA_S3_BUCKET")) / os.getenv(
                "FARS_KEY"
            )
            print("\nRunning on AWS using data from {data_path}\n")
            raise NotImplementedError()

        elif cmd == "Q":
            print("\nExiting")
            sys.exit(0)

    spark = create_spark_session()
    all_acc_df = read_csv("../../Data/FARS/CSV/FARS*NationalCSV/ACCIDENT.CSV")
    print(f"accident count: {all_acc_df.count():,}")

    pb_df = read_csv(
        [
            "../../Data/FARS/CSV/FARS2014NationalCSV/PBTYPE.CSV",
            "../../Data/FARS/CSV/FARS2015NationalCSV/PBTYPE.CSV",
            "../../Data/FARS/CSV/FARS2016NationalCSV/PBTYPE.CSV",
            "../../Data/FARS/CSV/FARS2017NationalCSV/PBTYPE.CSV",
            "../../Data/FARS/CSV/FARS2018NationalCSV/PBTYPE.CSV",
        ]
    )

    acc_with_pb_df = read_csv(
        [
            "../../Data/FARS/CSV/FARS2014NationalCSV/ACCIDENT.CSV",
            "../../Data/FARS/CSV/FARS2015NationalCSV/ACCIDENT.CSV",
            "../../Data/FARS/CSV/FARS2016NationalCSV/ACCIDENT.CSV",
            "../../Data/FARS/CSV/FARS2017NationalCSV/ACCIDENT.CSV",
            "../../Data/FARS/CSV/FARS2018NationalCSV/ACCIDENT.CSV",
        ]
    )
    print(f"pb_df.count() -> {pb_df.count():,}")
    join_expression = acc_with_pb_df["ST_CASE"] == pb_df["ST_CASE"]
    pb_acc_df = pb_df.join(acc_with_pb_df, join_expression, how="left")
    print(f"acc_pb_df.count() -> {pb_acc_df.count():,}")

    # all accidents with consistent coding
    all_acc_aux_df = read_csv(
        "../../Data/FARS/CSV/FARS*NationalCSV/ACC_AUX.CSV"
    )

    # inner join acc_aux and accident dfs
    all_acc_df.createOrReplaceTempView("all_acc_view")
    all_acc_aux_df.createOrReplaceTempView("acc_aux_view")

    # loop over directories with accident.csv and acc_aux.csv files
    print(f"data_path={data_path}")
    dirs = find_dirs_with_both_files("ACCIDENT.CSV", "ACC_AUX.CSV", data_path)
    print(f"len(dirs)={len(dirs)}")

    # join each pair together
    acc_dfs = []
    for _dir in dirs:
        accident_df = read_csv(str(Path(_dir).joinpath("ACCIDENT.CSV")),)
        print(f"accident_df: {accident_df.count()}, {accident_df.columns}")

        acc_aux_df = read_csv(str(Path(_dir).joinpath("ACC_AUX.CSV")))
        print(f"acc_aux_df: {acc_aux_df.count()}\n{acc_aux_df.columns}")

        acc_df = accident_df.join(acc_aux_df, on="ST_CASE")
        print(f"acc_df: {acc_df.count()}\n{acc_df.columns}")

        acc_dfs.append(acc_df)

    # append them together
    print(f"number of joined df: {len(acc_dfs)}")
    all_acc_df = reduce(DataFrame.unionByName, acc_dfs)
    print(f"#rows in all_acc_df{all_acc_df.count():,}")
    print(f"cols: {all_acc_df.columns}")

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
