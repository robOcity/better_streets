import os
import sys
from pathlib import Path
from functools import reduce
from pyspark.sql import (
    SparkSession,
    DataFrame,
)
from dotenv import load_dotenv


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

    # TODO cleanup
    print("In get_command()")
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


def get_root_dir(env="DATA_LOCAL_ROOT"):
    """Returns the root data directory."""

    return Path(os.getenv(env))


def get_raw_path(env="DATA_LOCAL_ROOT"):
    """Returns the path to the 'raw' data directory containing unprocessed data."""

    return get_root_dir(env) / "raw"


def get_interim_data_path(env="DATA_LOCAL_ROOT"):
    """Returns the path to the 'interim' data directory containing in-production data."""

    return get_root_dir(env) / "interim"


def get_processed_data_path(env="DATA_LOCAL_ROOT"):
    """Returns the path to the 'processed' data directory containing fully-processed data."""

    return get_root_dir(env) / "processed"


def get_fars_path(env="DATA_LOCAL_ROOT"):
    """Returns the path to the top-level FARS directory."""

    return get_root_dir(env) / os.getenv("FARS_KEY")


def get_S3_path(bucket):
    """Returns a list of folders in an S3 bucket."""

    raise NotImplementedError("get_S3_paths")


def main():
    """Extracts, transforms and loads the traffic accident data."""

    env_path = Path(".") / ".env"
    load_dotenv(dotenv_path=env_path, verbose=True)

    assert (os.getenv("DATA_LOCAL_ROOT") is not None) and (
        os.getenv("DATA_S3_BUCKET") is not None
    ), "Environment variable with the root data directory has not been set"

    # TODO clean-up
    print("Main before while")
    while True:
        cmd = get_command()
        if cmd == "L":
            raw_fars_data_path = Path(os.getenv("DATA_LOCAL_ROOT")) / os.getenv(
                "FARS_KEY"
            )

            make_filenames_case_consistent(get_fars_path())
            print(f"\nRunning locally using FARS data from {get_fars_path()}\n")
            break

        elif cmd == "A":
            raise NotImplementedError()

        elif cmd == "Q":
            print("\nExiting")
            sys.exit(0)

    spark = create_spark_session()

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
            "ST_CASE",
            "COUNTY",
            "STATE",
            "CITY",
            "YEAR",
            "MONTH",
            "DAY",
            "HOUR",
            "MINUTE",
            "DAY_WEEK",
            "LGT_COND",
            "FATALS",
        )
        accident_df = fix_spaces_in_column_names(accident_df)
        accident_dfs.append(accident_df)

        # read in csv and only keep columns common to all years
        acc_aux_df = read_csv(str(Path(_dir).joinpath("ACC_AUX.CSV"))).select(
            "ST_CASE",
            "FATALS",
            "YEAR",
            "STATE",
            "COUNTY",
            "A_DROWSY",
            "A_PEDAL",
            "A_ROLL",
            "A_MANCOL",
            "A_TOD",
            "A_CRAINJ",
            "A_RD",
            "A_RELRD",
            "A_LT",
            "A_POSBAC",
            "A_CT",
            "A_INTSEC",
            "A_HR",
            "A_DOW",
            "A_SPCRA",
            "A_REGION",
            "A_INTER",
            "A_DIST",
            "A_JUNC",
            "A_PED",
            "A_POLPUR",
            "A_MC",
            "A_RU",
            "A_ROADFC",
            "A_D15_19",
            "A_D15_20",
            "A_D16_19",
            "A_D16_20",
            "A_D16_24",
            "A_D21_24",
            "A_D65PLS",
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
        print("Common ACCIDENT.CSV Columns:", accident_common_cols)

        acc_aux_common_cols = find_common_set_of_column_names(acc_aux_dfs)
        print("Common ACC_AUX.CSV Columns:", acc_aux_common_cols)

        # append combined accident files for all years
        all_acc_df = reduce(DataFrame.unionByName, acc_dfs)

    except Exception:
        print(f"Excepetion while processing: {_dir}")
        print(f"Use only common ACCIDENT.CSV columns :\n{accident_common_cols}")
        print(f"Use only common ACC_AUX.CSV columns:\n{acc_aux_common_cols}")

    # show the number of records
    print(
        f"\nNumber of motor vehicle accidents (1982-2018): {all_acc_df.count():,}"
    )

    # save resulting dataframe for analysis
    output_path = str(
        get_interim_data_path().joinpath("all_accidents_1982_to_2018.csv")
    )
    print(f"output_path={output_path}")
    all_acc_df.write.csv(output_path, mode="overwrite", header=True)


if __name__ == "__main__":
    print("About to run main()")
    main()
