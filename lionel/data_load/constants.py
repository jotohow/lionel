from pathlib import Path

BASE = Path(__file__).parents[2]
DATA = BASE / "data"
RAW = DATA / "raw"
PROCESSED = DATA / "processed"
CLEANED = DATA / "cleaned"

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

TEAM_MAP = {
    "team": {
        "Man City": "Manchester City",
        "Man Utd": "Manchester Utd",
        "Spurs": "Tottenham",
        "Nott'm Forest": "Nottingham",
    }
}
