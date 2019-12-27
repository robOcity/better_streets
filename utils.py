import os
from pathlib import Path
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame


def create_spark_session():
    """Return a SparkSession object."""

    return SparkSession.builder.getOrCreate()


def assert_environment_is_good():
    assert (os.getenv("DATA_LOCAL_ROOT") is not None) and (
        os.getenv("DATA_S3_BUCKET") is not None
    ), "Environment variable with the root data directory has not been set"


def read_csv(csv_full_path):
    """Returns the PySpark (v2.x) SessionSession object."""

    return create_spark_session().read.csv(
        csv_full_path,
        header=True,
        inferSchema=True,
        enforceSchema=False,
        mode="DROPMALFORMED",
    )


def write_csv(df, path):
    """Saves the dataframe as partitioned CSV files under the specified path."""

    df.write.csv(path, mode="overwrite", header=True)


def get_dir(root, project, kind, source):
    return Path(root).joinpath(project).joinpath(kind).joinpath(source)


def get_root_dir(root="DATA_LOCAL_ROOT", project="PROJECT_KEY"):
    """Returns the root data directory."""

    return Path(os.getenv(root)).joinpath(os.getenv(project))


def get_raw_path(env="DATA_LOCAL_ROOT"):
    """Returns the path to the 'raw' data directory containing unprocessed data."""

    return get_root_dir(env) / "raw"


def get_S3_path(bucket):
    """Returns a list of folders in an S3 bucket."""

    raise NotImplementedError("get_S3_paths")


def convert_dms_to_dd(degrees, minutes, seconds):
    """Return a decimal degrees value by converting from degrees, minutes, seconds.
    
    Cite: https://gist.github.com/tsemerad/5053378"""

    return degrees + float(minutes) / 60 + float(seconds) / 3600
