import utils
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import functions as F


def main():
    env_path = Path(".") / ".env"
    load_dotenv(dotenv_path=env_path, verbose=True)

    # fail in enviroment has not been set
    utils.assert_environment_is_good()

    # get spark sesssion object
    spark = utils.create_spark_session()

    # read in accident data
    full_path = str(
        utils.get_interim_data_path() / "all_accidents_1982_to_2018.csv"
    )
    accidents = utils.read_csv(full_path)

    # prepare for analysis
    accidents.createOrReplaceTempView("accidents")

    # total number of accidents
    print(f"Fatal Accidents 1982 to 1018: {accidents.count():,}")

    # read in geographic location codes as json
    glc_path = str(
        utils.get_external_data_path(
            src_dir="FRPP_GLC", filename="FRPP_GLC_United_States.json"
        )
    )

    location = spark.read.json(
        glc_path, mode="FAILFAST", multiLine=True, allowNumericLeadingZero=True
    )
    location.show(5)
    location.createOrReplaceTempView("location")

    # join the GLC and FARS dataframes
    acc_w_loc = spark.sql(
        """
        SELECT * 
        FROM accidents a
        JOIN location l
        ON (a.STATE = l.State_Code AND
        a.COUNTY = l.County_Code AND
        a.CITY = l.City_Code)
        """
    )
    acc_w_loc.show(5)

    # accidents per year in denver, co and seattle, wa
    acc_w_loc.select("YEAR", "City_Name", "FATALS").filter(
        F.lower(acc_w_loc.City_Name).contains("denver")
    ).show()

    # TODO per capita accidents per year in denver, co and seattle, wa

    # TODO uncomment to save
    # awl_path = str(
    #     utils.get_interim_data_path() / "accidents_with_location.csv"
    # )
    # utils.write_csv(accidents_with_location, awl_path)


if __name__ == "__main__":
    main()
