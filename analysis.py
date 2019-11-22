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

    # accidents per year in denver, co and seattle, wa

    # per capita accidents per year in denver, co and seattle, wa


if __name__ == "__main__":
    main()