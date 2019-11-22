import utils
from pathlib import Path
from dotenv import load_dotenv


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
    accidents_with_location = spark.sql(
        """
        SELECT * 
        FROM accidents a
        JOIN location l
        ON (a.STATE = l.State_Code AND
        a.COUNTY = l.County_Code AND
        a.CITY = l.City_Code)
        """
    )
    accidents_with_location.show(5)
    awl_path = str(
        utils.get_interim_data_path() / "accidents_with_location.csv"
    )
    # utils.write_csv(accidents_with_location, awl_path)
    accidents_with_location.createOrReplaceTempView("accidents_with_location")

    # accidents per year in denver, co and seattle, wa
    # TODO - get query working or use api
    spark.sql(
        f"""
        SELECT YEAR, City_Name, State_Name
        FROM accidents_with_location
        WHERE State_Code = 53
        """
    ).show()

    # TODO per capita accidents per year in denver, co and seattle, wa


if __name__ == "__main__":
    main()
