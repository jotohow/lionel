from pathlib import Path

BASE = Path(__file__).parents[2]
DATA = BASE / "data"
RAW = DATA / "raw"

BASE_URL = (
    "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"
)

SEASON_MAP = {
    24: "2023-24",
    23: "2022-23",
    22: "2021-22",
    21: "2020-21",
    20: "2019-20",
    19: "2018-19",
}
