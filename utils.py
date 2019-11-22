import os
from pathlib import Path
from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame


def create_spark_session():
    """Return a SparkSession object."""

    # TODO clean-up
    # spark = SparkSession.builder.config(
    #     "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    # ).getOrCreate()
    spark = SparkSession.builder.getOrCreate()
    return spark


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

    df.csv(path, mode="overwrite", header=True)


def get_root_dir(env="DATA_LOCAL_ROOT"):
    """Returns the root data directory."""

    return Path(os.getenv(env))


def get_raw_path(env="DATA_LOCAL_ROOT"):
    """Returns the path to the 'raw' data directory containing unprocessed data."""

    return get_root_dir(env) / "raw"


def get_interim_data_path(env="DATA_LOCAL_ROOT"):
    """Returns the path to the 'interim' data directory containing in-production data."""

    return get_root_dir(env) / "interim"


def get_external_data_path(src_dir=None, filename=None, env="DATA_LOCAL_ROOT"):
    """Returns the path to the 'interim' data directory containing in-production data."""

    return get_root_dir(env) / "external" / src_dir / filename


def get_processed_data_path(env="DATA_LOCAL_ROOT"):
    """Returns the path to the 'processed' data directory containing fully-processed data."""

    return get_root_dir(env) / "processed"


def get_fars_path(env="DATA_LOCAL_ROOT"):
    """Returns the path to the top-level FARS directory."""

    return get_root_dir(env) / os.getenv("FARS_KEY")


def get_S3_path(bucket):
    """Returns a list of folders in an S3 bucket."""

    raise NotImplementedError("get_S3_paths")
