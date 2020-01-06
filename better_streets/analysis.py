import os
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql import types as T
import utils


def main():
    """Analysis of pedestrian and cyclist fatalities from 1982 to 2018."""

    # load and check environment has been set
    utils.load_env()
    root, project = (
        os.getenv("DATA_ROOT"),
        os.getenv("PROJECT_KEY"),
    )

    # get spark session object
    spark = utils.create_spark_session()

    # read in accident data
    full_path = utils.get_dir(root, project, "interim", "FARS").joinpath(
        "all_fatal_accidents_1982_to_2018.csv"
    )
    accidents = utils.read_csv(full_path)

    # convert column to integer
    accidents = accidents.withColumn(
        "FATALS", accidents["FATALS"].cast(T.IntegerType())
    )

    # prepare for analysis
    accidents.createOrReplaceTempView("accidents")

    # total number of accidents
    print(f"\nFatal Accidents 1982 to 1018: {accidents.count():,}")
    # Fatal Accidents 1982 to 1018: 1,349,445

    # read in geographic location codes as json
    glc_path = utils.get_dir(root, project, "external", "FRPP_GLC").joinpath(
        "FRPP_GLC_United_States.json"
    )

    location = spark.read.json(
        str(glc_path), mode="FAILFAST", multiLine=True, allowNumericLeadingZero=True,
    )
    location.show(5)
    # +---------+--------------+------------+-----------+-----------+-----------------+-------------+----------+----------+---------+
    # |City_Code|     City_Name|Country_Code|County_Code|County_Name|Date_Record_Added|Old_City_Name|State_Code|State_Name|Territory|
    # +---------+--------------+------------+-----------+-----------+-----------------+-------------+----------+----------+---------+
    # |     0010|     ABBEVILLE|         840|        067|      HENRY|                 |             |        01|   ALABAMA|        U|
    # |     0050|   ALBERTVILLE|         840|        095|   MARSHALL|                 |             |        01|   ALABAMA|        U|
    # |     0060|ALEXANDER CITY|         840|        123| TALLAPOOSA|                 |             |        01|   ALABAMA|        U|
    # |     0070|    ALICEVILLE|         840|        107|    PICKENS|                 |             |        01|   ALABAMA|        U|
    # |     0090|     ANDALUSIA|         840|        039|  COVINGTON|                 |             |        01|   ALABAMA|        U|
    # +---------+--------------+------------+-----------+-----------+-----------------+-------------+----------+----------+---------+
    # only showing top 5 rows

    location.createOrReplaceTempView("location")

    # join the GLC and FARS dataframes and limit
    # scope to denver/seattle
    den_sea_fatalities = spark.sql(
        """
        SELECT a.YEAR as Year, l.City_Name, sum(a.FATALS) as All_Fatalities
        FROM accidents a
        JOIN location l
        ON (a.STATE = l.State_Code AND
        a.COUNTY = l.County_Code AND
        a.CITY = l.City_Code)
        WHERE (l.State_Code = '08' AND l.City_Code = '0600') OR
        (l.State_Code = '53' AND l.City_Code = '1960')
        GROUP BY a.YEAR, l.City_Name
        ORDER BY a.YEAR
        """
    )
    den_sea_fatalities.show(5)
    # +----+---------+--------------+
    # |Year|City_Name|All_Fatalities|
    # +----+---------+--------------+
    # |1982|  SEATTLE|            53|
    # |1982|   DENVER|            68|
    # |1983|  SEATTLE|            50|
    # |1983|   DENVER|            55|
    # |1984|  SEATTLE|            45|
    # +----+---------+--------------+
    # only showing top 5 rows

    # save the results
    den_sea_fatalities_path = (
        utils.get_dir(root, project, "processed", "FARS") / "den_sea_fatalities.csv"
    )

    utils.write_csv(den_sea_fatalities, den_sea_fatalities_path)

    # now just pedestrian and bicycle accidents
    den_sea_ped_bike_fatalities = spark.sql(
        """
        SELECT a.YEAR as Year, l.City_Name, sum(a.FATALS) as Ped_Bike_Fatalities
        FROM accidents a
        JOIN location l
        ON (a.STATE = l.State_Code AND
        a.COUNTY = l.County_Code AND
        a.CITY = l.City_Code)
        WHERE ((l.State_Code = '08' AND l.City_Code = '0600') OR
        (l.State_Code = '53' AND l.City_Code = '1960')) AND
        (a.A_PED = 1 OR a.A_PEDAL = 1)
        GROUP BY a.YEAR, l.City_Name
        ORDER BY a.YEAR
        """
    )
    den_sea_ped_bike_fatalities.show(5)
    # +----+---------+-----------+
    # |YEAR|City_Name|sum(FATALS)|
    # +----+---------+-----------+
    # |1982|  SEATTLE|         17|
    # |1982|   DENVER|         24|
    # |1983|  SEATTLE|         20|
    # |1983|   DENVER|         13|
    # |1984|  SEATTLE|         16|
    # +----+---------+-----------+
    # only showing top 5 rows

    # save the results
    den_sea_ped_bike_fatalities_path = utils.get_dir(
        root, project, "processed", "FARS"
    ).joinpath("den_sea_ped_bike_fatalities.csv")

    utils.write_csv(den_sea_ped_bike_fatalities, den_sea_ped_bike_fatalities_path)


if __name__ == "__main__":
    main()