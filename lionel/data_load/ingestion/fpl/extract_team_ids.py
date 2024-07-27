import pandas as pd
from pathlib import Path

BASE = Path(__file__).parents[4]
DATA = BASE / "data"
RAW = DATA / "raw"
RAW = Path("/Users/toby/Dev/lionel/data/raw")

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


def get_team_ids(season):
    team_ids = pd.read_csv(f"{BASE_URL}/{SEASON_MAP[season]}/teams.csv")
    team_ids["season"] = season
    return team_ids


def update_local_team_ids(season):
    df_team_ids = get_team_ids(season)
    df_team_ids.to_csv(RAW / f"team_ids_{season}.csv", index=False)


if __name__ == "__main__":
    update_local_team_ids(24)
