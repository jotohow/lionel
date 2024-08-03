import pandas as pd
from pathlib import Path
from lionel.data_load.constants import RAW, BASE_URL, SEASON_MAP, TEAM_MAP


def get_team_ids(season):
    team_ids = pd.read_csv(f"{BASE_URL}/{SEASON_MAP[season]}/teams.csv")
    team_ids["season"] = season
    return team_ids


def update_local_team_ids(season):
    df_team_ids = get_team_ids(season)
    df_team_ids.to_csv(RAW / f"team_ids_{season}.csv", index=False)


def clean_team_ids(team_ids):
    team_ids = team_ids.rename(columns={"name": "team_name", "id": "team_id"})
    team_ids = team_ids.replace(TEAM_MAP)
    return team_ids


if __name__ == "__main__":
    update_local_team_ids(24)
