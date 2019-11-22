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

    # TODO cleanup after getting
    # create dataframe of cities of interest
    columns = ["City", "County", "State", "GNIS_ID"]
    values = [
        ("Denver", "Denver", "Colorado", "2410324"),
        ("Seattle", "King", "Washington", "2411856"),
    ]
    city_df = spark.createDataFrame(values, columns)
    city_df.show()

    # read in geographic location codes as json
    glc_path = str(
        utils.get_external_data_path(
            src_dir="FRPP_GLC", filename="FRPP_GLC_United_States.json"
        )
    )

    # schema =
    print(f"glc_path={glc_path}")
    glc_df = spark.read.json(
        glc_path, mode="FAILFAST", multiLine=True, allowNumericLeadingZero=True
    )
    glc_df.show(5)

    # accidents per year in denver, co and seattle, wa
    spark.sql(
        """
    SELECT count(*) 
    FROM accidents
    WHERE STATE = 08 AND COUNTY = 031 AND CITY = 0600
    """
    ).show()

    # per capita accidents per year in denver, co and seattle, wa


if __name__ == "__main__":
    main()
