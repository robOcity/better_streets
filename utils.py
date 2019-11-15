from pathlib import Path
from dotenv import load_dotenv
from pathlib import Path
import os


def list_files(root, filename, extension):
    path = Path(root)
    return list(path.glob(f"**/{filename}.{extension}"))


def main():
    # TODO create test
    # 2015 has mixed case file names: acc_aux.csv, Factor.csv, ACCIDENT.csv
    env_path = Path(".") / ".env"
    load_dotenv(dotenv_path=env_path, verbose=True)
    data_root = os.getenv("DATA_PATH_LOCAL")
    city_lat_lon_key = os.getenv("CITY_LAT_LON_KEY")
    fars_key = os.getenv("FARS_KEY")
    file_list = list_files(data_root, "ACC_AUX", "CSV")
    print("\n".join([str(p) for p in sorted(file_list)]))
    print(len(file_list))
    assert len(file_list) == 37, "Case insensetive file handling is lossing data files"


if __name__ == "__main__":
    main()
