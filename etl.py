from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import (
    SparkSession,
    DataFrameReader,
    DataFrame,
)
from pyspark.sql.utils import AnalysisException
from dotenv import load_dotenv
from pathlib import Path
from functools import reduce
import os
import sys


def create_spark_session():
    """Return a SparkSession object."""

    # TODO clean-up
    # spark = SparkSession.builder.config(
    #     "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    # ).getOrCreate()
    spark = SparkSession.builder.getOrCreate()
    return spark


def get_command():
    """Return the command from the user."""

    cmd = ""
    while cmd not in ["L", "A", "Q"]:
        cmd = input("\nL - [L]ocal\nA - [A]WS\nQ - [Q]uit\nCommand: ")[
            0
        ].upper()
    return cmd


def make_filenames_case_consistent(data_path):
    """Returns a list of Path objects with consistent upper-case names."""

    return [
        p.rename(p.parent / p.name.upper())
        for p in Path(data_path).rglob("*.csv")
    ]


def find_dirs_with_both_files(file_1, file_2, data_path):
    """Returns a list of directories that contain both file_1 and file_2."""

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
    """Returns the PySpark (v2.x) SessionSession object."""

    return create_spark_session().read.csv(
        csv_full_path,
        header=True,
        inferSchema=True,
        enforceSchema=False,
        mode="DROPMALFORMED",
    )

    # find the columns that are not common


def find_common_set_of_column_names(dfs):
    """Returns the set of columns common to the list of dataframes provided."""

    cols = [set(df.columns) for df in dfs]
    common_cols = list(reduce(set.intersection, map(set, cols)))
    return common_cols


def fix_spaces_in_column_names(df):
    """Returns the dataframe provided with column names without spaces."""

    new_names = []
    for col in df.columns:
        new_name = col.strip()
        new_name = "".join(new_name.split())
        new_name = new_name.replace(".", "")
        new_names.append(new_name)
    return df.toDF(*new_names)


def get_S3_paths(bucket, pattern):
    """Returns a list of folders in an S3 bucket."""

    raise NotImplementedError("get_S3_paths")


def main():
    """Extracts, transforms and loads the traffic accident data."""

    env_path = Path(".") / ".env"
    load_dotenv(dotenv_path=env_path, verbose=True)

    assert (os.getenv("DATA_LOCAL_ROOT") is not None) and (
        os.getenv("DATA_S3_BUCKET") is not None
    ), "Environment variable with the root data directory has not been set"

    raw_fars_data_path = ""

    while True:
        cmd = get_command()
        if cmd == "L":
            raw_fars_data_path = Path(os.getenv("DATA_LOCAL_ROOT")) / os.getenv(
                "FARS_KEY"
            )
            print(f"raw_fars_data_path: {raw_fars_data_path}")
            city_lat_lon_key = os.getenv("CITY_LAT_LON_KEY")
            fars_key = os.getenv("FARS_KEY")
            make_filenames_case_consistent
            print(
                f"\nRunning locally using FARS data from {raw_fars_data_path}\n"
            )
            break

        elif cmd == "A":
            raw_fars_data_path = Path(os.getenv("DATA_S3_BUCKET")) / os.getenv(
                "FARS_KEY"
            )
            print("\nRunning on AWS using data from {raw_fars_data_path}\n")
            raise NotImplementedError()

        elif cmd == "Q":
            print("\nExiting")
            sys.exit(0)

    spark = create_spark_session()

    # join the dataframes where many peds/cyclists can be involved in a single accident
    join_expression = recent_accident_df["ST_CASE"] == recent_pb_df["ST_CASE"]
    pb_acc_df = recent_pb_df.join(
        recent_accident_df, join_expression, how="left_outer"
    )
    print(
        f"\nNumber of pedestrians & cyclists hit (2014-2018): {pb_acc_df.count():,}"
    )

    # loop over directories with accident.csv and acc_aux.csv files
    dirs = find_dirs_with_both_files(
        "ACCIDENT.CSV", "ACC_AUX.CSV", raw_fars_data_path
    )
    count = 1
    accident_dfs, acc_aux_dfs, acc_dfs = [], [], []

    # join each pair together
    print("\nProcessing Directories")
    for _dir in dirs:
        # read in csv data and keep columns common to all years
        accident_df = read_csv(str(Path(_dir).joinpath("ACCIDENT.CSV"))).select(
            "WEATHER",
            "MILEPT",
            "HARM_EV",
            "COUNTY",
            "DAY",
            "RAIL",
            "NOT_HOUR",
            "NOT_MIN",
            "CITY",
            "ST_CASE",
            "DAY_WEEK",
            "PERSONS",
            "MINUTE",
            "HOUR",
            "ARR_MIN",
            "YEAR",
            "SP_JUR",
            "MONTH",
            "ARR_HOUR",
            "DRUNK_DR",
            "REL_ROAD",
            "VE_FORMS",
            "LGT_COND",
            "FATALS",
            "STATE",
        )
        accident_df = fix_spaces_in_column_names(accident_df)
        accident_dfs.append(accident_df)

        # read in csv and only keep columns common to all years
        acc_aux_df = read_csv(str(Path(_dir).joinpath("ACC_AUX.CSV"))).select(
            "A_D21_24",
            "A_D15_20",
            "STATE",
            "A_DROWSY",
            "A_PEDAL",
            "A_ROLL",
            "A_MANCOL",
            "A_D65PLS",
            "A_TOD",
            "YEAR",
            "A_CRAINJ",
            "A_RD",
            "ST_CASE",
            "A_RELRD",
            "A_LT",
            "FATALS",
            "A_POSBAC",
            "A_CT",
            "COUNTY",
            "A_D16_19",
            "A_INTSEC",
            "A_HR",
            "A_DOW",
            "A_D16_20",
            "A_SPCRA",
            "A_D16_24",
            "A_REGION",
            "A_INTER",
            "A_DIST",
            "A_JUNC",
            "A_PED",
            "A_D15_19",
            "A_POLPUR",
            "A_MC",
            "A_RU",
            "A_ROADFC",
        )
        acc_aux_df = fix_spaces_in_column_names(acc_aux_df)
        acc_aux_dfs.append(acc_aux_df)

        # join dataframes and drop duplicated columns after merge
        acc_df = accident_df.join(acc_aux_df, on="ST_CASE").drop(
            "STATE", "YEAR", "COUNTY", "FATALS",
        )
        acc_dfs.append(acc_df)

        print(
            f"{count} dir:{_dir}  rows: {acc_df.count():,}  cols: {len(acc_df.columns):,}"
        )
        count += 1

    try:
        # find columns common to all years
        accident_common_cols = find_common_set_of_column_names(accident_dfs)
        acc_aux_common_cols = find_common_set_of_column_names(acc_aux_dfs)

        # append combined accident files for all years
        all_acc_df = reduce(DataFrame.unionByName, acc_dfs)
    except Exception:
        print(f"Excepetion while processing: {_dir}", file=sys.stderr)
        print(
            f"Use common columns in ACCIDENT.CSV files:\n{accident_common_cols}",
            file=sys.stderr,
        )
        print(
            f"Use common columns in ACC_AUX.CSV files:\n{acc_aux_common_cols}",
            file=sys.stderr,
        )

    # read in pedestrain / cyclist data for recent years with consistent data labeling
    recent_pb_df = read_csv(
        [
            "../../Data/external/FARS/CSV/FARS2014NationalCSV/PBTYPE.CSV",
            "../../Data/external/FARS/CSV/FARS2015NationalCSV/PBTYPE.CSV",
            "../../Data/external/FARS/CSV/FARS2016NationalCSV/PBTYPE.CSV",
            "../../Data/external/FARS/CSV/FARS2017NationalCSV/PBTYPE.CSV",
            "../../Data/external/FARS/CSV/FARS2018NationalCSV/PBTYPE.CSV",
        ]
    )
    recent_pb_df = fix_spaces_in_column_names(recent_pb_df)

    # read in accident data for recent years and columns common to all years
    recent_accident_df = read_csv(
        [
            "../../Data/external/FARS/CSV/FARS2014NationalCSV/ACCIDENT.CSV",
            "../../Data/external/FARS/CSV/FARS2015NationalCSV/ACCIDENT.CSV",
            "../../Data/external/FARS/CSV/FARS2016NationalCSV/ACCIDENT.CSV",
            "../../Data/external/FARS/CSV/FARS2017NationalCSV/ACCIDENT.CSV",
            "../../Data/external/FARS/CSV/FARS2018NationalCSV/ACCIDENT.CSV",
        ]
    ).select(
        "WEATHER",
        "MILEPT",
        "HARM_EV",
        "COUNTY",
        "DAY",
        "RAIL",
        "NOT_HOUR",
        "NOT_MIN",
        "CITY",
        "ST_CASE",
        "DAY_WEEK",
        "PERSONS",
        "MINUTE",
        "HOUR",
        "ARR_MIN",
        "YEAR",
        "SP_JUR",
        "MONTH",
        "ARR_HOUR",
        "DRUNK_DR",
        "REL_ROAD",
        "VE_FORMS",
        "LGT_COND",
        "FATALS",
        "STATE",
    )
    recent_accident_df = fix_spaces_in_column_names(recent_accident_df)
    print(
        f"\nNumber of motor vehicle accidents involving ped / cyclists (2014-2018): {recent_accident_df.count():,}"
    )

    # show the number of records
    print(
        f"\nNumber of motor vehicle accidents (1982-2018): {all_acc_df.count():,}"
    )

    # save resulting dataframe for analysis


if __name__ == "__main__":
    main()
