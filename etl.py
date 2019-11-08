from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import SparkSession, DataFrameReader


def create_spark_session():
    """Return a SparkSession object."""
    # spark = SparkSession.builder.config(
    #     "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    # ).getOrCreate()
    spark = SparkSession.builder.getOrCreate()
    # TODO clean-up
    print(f"\ncreate_spark_session() --> {type(spark)}\n")
    return spark


if __name__ == "__main__":
    # STATE,COUNTY,MONTH,DAY,HOUR,MINUTE,VE_FORMS,PERSONS,PEDS,NHS,ROAD_FNC,ROUTE,SP_JUR,HARM_EV,MAN_COLL,REL_JUNC,REL_ROAD,TRAF_FLO,NO_LANES,SP_LIMIT,ALIGNMNT,PROFILE,PAVE_TYP,SUR_COND,TRA_CONT,T_CONT_F,HIT_RUN,LGT_COND,WEATHER,C_M_ZONE,NOT_HOUR,NOT_MIN,ARR_HOUR,ARR_MIN,HOSP_HR,HOSP_MN,SCH_BUS,CF1,CF2,CF3,FATALS,DAY_WEEK,DRUNK_DR,ST_CASE,CITY,MILEPT,YEAR,TWAY_ID,RAIL,LATITUDE,LONGITUD
    schema = T.StructType(
        [
            T.StructField("STATE", T.IntegerType(), True),
            T.StructField("COUNTY", T.IntegerType(), True),
            T.StructField("MONTH", T.IntegerType(), True),
            T.StructField("DAY", T.IntegerType(), True),
            T.StructField("YEAR", T.IntegerType(), True),
            T.StructField("HOUR", T.IntegerType(), True),
            T.StructField("MINUTE", T.IntegerType(), True),
            T.StructField("VE_FORMS", T.IntegerType(), True),
            T.StructField("PERSONS", T.IntegerType(), True),
            T.StructField("VEHICLES", T.IntegerType(), True),
            T.StructField("LAND_USE", T.IntegerType(), True),
            T.StructField("CL_TWAY", T.IntegerType(), True),
            T.StructField("ROAD_FNC", T.IntegerType(), True),
            T.StructField("TA_1_CL", T.IntegerType(), True),
            T.StructField("SP_JUR", T.IntegerType(), True),
            T.StructField("HARM_EV", T.IntegerType(), True),
            T.StructField("MAN_COLL", T.IntegerType(), True),
            T.StructField("REL_JUNC", T.IntegerType(), True),
            T.StructField("REL_ROAD", T.IntegerType(), True),
            T.StructField("ROAD_FLO", T.IntegerType(), True),
            T.StructField("NO_LANES", T.IntegerType(), True),
            T.StructField("SP_LIMIT", T.IntegerType(), True),
            T.StructField("ALIGNMNT", T.IntegerType(), True),
            T.StructField("PROFILE", T.IntegerType(), True),
            T.StructField("PAVE_TYP", T.IntegerType(), True),
            T.StructField("SUR_COND", T.IntegerType(), True),
            T.StructField("TRA_CONT", T.IntegerType(), True),
            T.StructField("LGT_COND", T.IntegerType(), True),
            T.StructField("WEATHER", T.IntegerType(), True),
            T.StructField("HIT_RUN", T.IntegerType(), True),
            T.StructField("C_M_ZONE", T.IntegerType(), True),
            T.StructField("NOT_HOUR", T.IntegerType(), True),
            T.StructField("NOT_MIN", T.IntegerType(), True),
            T.StructField("ARR_HOUR", T.IntegerType(), True),
            T.StructField("ARR_MIN", T.IntegerType(), True),
            T.StructField("SCH_BUS", T.IntegerType(), True),
            T.StructField("CF1", T.IntegerType(), True),
            T.StructField("CF2", T.IntegerType(), True),
            T.StructField("CF3", T.IntegerType(), True),
            T.StructField("FATALS", T.IntegerType(), True),
            T.StructField("DAY_WEEK", T.StringType(), True),
            T.StructField("DRUNK_DR", T.StringType(), True),
            T.StructField("ST_CASE", T.StringType(), True),
            T.StructField("CITY", T.StringType(), True),
            T.StructField("RAIL", T.StringType(), True),
        ]
    )

    spark = create_spark_session()
    # one csv: ../../Data/FARS/CSV/FARS2018NationalCSV/ACCIDENT.csv
    # all csv: ../../Data/FARS/CSV/FARS*NationalCSV/ACCIDENT.csv
    df = spark.read.csv(
        "../../Data/FARS/CSV/FARS*NationalCSV/ACCIDENT.csv",
        header=True,
        inferSchema=True,
        multiLine=True,
        mode="DROPMALFORMED",
    )

print(f"accident count: {df.count():,}")
