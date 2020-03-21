import os
from pathlib import Path
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame


def create_spark_session():
    """Return a SparkSession object."""

    return SparkSession.builder.getOrCreate()


def load_env():
    env_path = Path(".") / ".env"
    load_dotenv(dotenv_path=env_path, verbose=True)

    assert (
        os.getenv("DATA_ROOT") is not None
    ), "Environment variable with the root data directory has not been set"


def read_csv(path):
    """Returns the PySpark (v2.x) SessionSession object."""

    # note: paths need to be represented only as string in pyspark
    path = path if isinstance(path, str) else str(path)
    return create_spark_session().read.csv(
        path, header=True, inferSchema=True, enforceSchema=False, mode="DROPMALFORMED",
    )


def write_csv(df, path):
    """Saves the dataframe as partitioned CSV files under the specified path."""

    # note: paths need to be represented only as string in pyspark
    path = path if isinstance(path, str) else str(path)
    df.write.csv(path, mode="overwrite", header=True)


def get_dir(root, project, kind, source):
    return Path(root).joinpath(project).joinpath(kind).joinpath(source)


def get_S3_path(bucket):
    """Returns a list of folders in an S3 bucket."""

    raise NotImplementedError("get_S3_paths")


def convert_dms_to_dd(degrees, minutes, seconds):
    """Return a decimal degrees value by converting from degrees, minutes, seconds.
    
    Cite: https://gist.github.com/tsemerad/5053378"""

    return degrees + float(minutes) / 60 + float(seconds) / 3600
