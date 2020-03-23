import os
import re
import sys
from pathlib import Path
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from dotenv import load_dotenv
from peds import utils


def get_command():
    """Return the command from the user."""

    cmd = ""
    while cmd not in ["A", "P", "Q"]:
        cmd = input(
            """
            A - [A]ccident Pipeline
            P - [P]erson Pipeline
            Q - [Q]uit
            Command: """
        )[0].upper()
    return cmd


def make_filenames_case_consistent(data_path):
    """Returns a list of Path objects with consistent upper-case names."""

    return [p.rename(p.parent / p.name.upper()) for p in Path(data_path).rglob("*.csv")]


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

    # find the columns that are not common


def find_common_set_of_column_names(dfs):
    """Returns a sorted list of common columns names."""

    cols = [set(df.columns) for df in dfs]
    return sorted(list(reduce(set.intersection, map(set, cols))))


def fix_spaces_in_column_names(columns):
    """Returns the cleaned-up column names without spaces and in upper-case."""

    new_names = []
    for col in columns:
        new_name = col.strip()
        new_name = "".join(new_name.split())
        new_name = new_name.replace(".", "")
        new_name = new_name.upper()
        new_names.append(new_name)
    return new_names


def load_glc_codes():
    """Returns the Geographic Locator Codes (GLCs) for the U.S."""

    codes_path = (
        utils.get_dir(
            os.getenv("DATA_ROOT"), os.getenv("PROJECT_KEY"), "external", "FRPP_GLC"
        )
        / "FRPP_GLC_United_States.csv"
    )
    print(f"codes path = {codes_path}", file=sys.stderr)
    return SparkSession.builder.getOrCreate().read.csv(
        str(codes_path), inferSchema=True, header=True
    )


def build_dir_year_dict(dirs):
    """Returns a dict with path as the key and 4-digit year as the value."""

    yr_reg = re.compile(r"\d{4}")
    hits = [yr_reg.search(str(dir)) for dir in dirs]
    return {Path(_match.string): _match.group() for _match in hits if _match}


def extract_city_by_code(df, glc_city_code, glc_state_code):
    """Returns a dataframe containing records for the city and state codes."""

    return df.filter(
        (df.City_Code == glc_city_code) & (df.State_Code == glc_state_code)
    )


def accident_pipeline(root, project):
    """Run the accident pipeline and extract Denver and Seattle specifics."""

    # loop over directories with accident.csv and acc_aux.csv files
    fars_data_path = utils.get_dir(root, project, "external", os.getenv("FARS_KEY"))
    print(f"\nRunning locally using FARS data from {fars_data_path}\n")

    dir_yr_dict = build_dir_year_dict(
        find_dirs_with_both_files("ACCIDENT.CSV", "ACC_AUX.CSV", fars_data_path)
    )
    count = 1
    accident_dfs, acc_aux_dfs, acc_dfs = [], [], []

    # join each pair together
    print("\nProcessing Directories")
    # TODO use map reduce rather than looping
    for _dir, year in dir_yr_dict.items():
        # read in csv data and keep columns common to all years
        accident_df = utils.read_csv(Path(_dir).joinpath("ACCIDENT.CSV")).select(
            "ST_CASE", "CITY", "MONTH", "DAY", "HOUR", "MINUTE", "DAY_WEEK", "LGT_COND"
        )
        # fix minor year to year differences in column naming
        accident_df = accident_df.toDF(*fix_spaces_in_column_names(accident_df.columns))
        accident_dfs.append(accident_df)

        # data quality check #1
        assert accident_df.count() > 0, f"accident_df dataframe from {_dir} is empty!"

        # read in csv and only keep columns common to all years
        acc_aux_df = utils.read_csv(Path(_dir).joinpath("ACC_AUX.CSV")).select(
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
        acc_aux_df = acc_aux_df.toDF(*fix_spaces_in_column_names(acc_aux_df.columns))
        acc_aux_dfs.append(acc_aux_df)

        # data quality check #2
        assert acc_aux_df.count() > 0, f"acc_aux_df dataframe from {_dir} is empty!"

        # join dataframes and drop duplicated columns after merge
        acc_df = accident_df.join(acc_aux_df, on="ST_CASE")
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
        print(f"Exception while processing: {_dir}")
        print(f"Use only common ACCIDENT.CSV columns :\n{accident_common_cols}")
        print(f"Use only common ACC_AUX.CSV columns:\n{acc_aux_common_cols}")

    # show the number of records
    print(f"\nNumber of motor vehicle accidents (1982-2018): {all_acc_df.count():,}")

    # data quality check #3
    assert (
        all_acc_df.count() > 0
    ), "Combined accident and acc_aux table (all_acc_df) dataframe is empty!"

    # save resulting dataframe for analysis
    output_path = str(
        utils.get_dir(root, project, "interim", "FARS").joinpath(
            "all_fatal_accidents_1982_to_2018.csv"
        )
    )
    print(f"output_path={output_path}")
    all_acc_df.write.csv(output_path, mode="overwrite", header=True)


def person_pipeline(root, project):
    """Run the person-level data pipeline."""
    print("Running the Person-level pipeline")
    raise NotImplementedError()


def main():
    """Extracts, transforms and loads the traffic accident data."""

    cmd = get_command()
    if cmd == "Q":
        print("\nExiting")
        sys.exit(0)

    utils.load_env()
    root, project = (os.getenv("DATA_ROOT"), os.getenv("PROJECT_KEY"))
    spark = utils.create_spark_session()
    fars_data_path = utils.get_dir(root, project, "external", os.getenv("FARS_KEY"))
    make_filenames_case_consistent(fars_data_path)

    if cmd == "A":
        print(f"\nRunning FARS Accident Pipeline\n")
        accident_pipeline(root, project)

    elif cmd == "P":
        person_pipeline(root, project)


if __name__ == "__main__":
    print("About to run main()")
    main()
