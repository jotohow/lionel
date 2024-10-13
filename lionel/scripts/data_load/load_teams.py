import datetime as dt
import json

import pandas as pd

from lionel.constants import RAW, TEAM_MAP
from lionel.db.connector import DBManager


def load_scraped_teams():
    today = dt.datetime.today().strftime("%Y%m%d")
    p = RAW / f"scraped_data_{today}.json"
    data = json.load(open(p, "r"))
    team_map = data["team_map"]
    df_teams = pd.DataFrame([{"web_id": k, **v} for k, v in team_map.items()])
    df_teams = df_teams.replace({"name": TEAM_MAP["team_name"]})
    df_teams["web_id"] = df_teams["web_id"].astype(int)
    return df_teams[["web_id", "name"]]  # drop the stregth etc cols


def clean_teams(df_teams):
    return df_teams.replace({"name": TEAM_MAP["team_name"]})


def get_existing_teams(dbm: DBManager):
    q = "SELECT DISTINCT name FROM teams"
    return pd.read_sql(q, dbm.engine.raw_connection()).name.tolist()


# These three are nested - not a good way to do it...
def get_team_ids(dbm):
    q = "SELECT id, name FROM teams"
    teams = pd.read_sql(q, dbm.engine.raw_connection()).set_index("id")
    return {v["name"]: k for k, v in teams.to_dict(orient="index").items()}


def build_team_season_map(dbm, df_teams, season=25):
    team_ids = get_team_ids(dbm)
    df_teams["team_id"] = df_teams["name"].map(team_ids)
    assert df_teams.team_id.isnull().sum() == 0
    df_teams["season"] = season
    return df_teams


def add_new_team_seasons(dbm, df_teams, season=25):
    df_teams = build_team_season_map(dbm, df_teams, season)
    existing_team_seasons = pd.read_sql(
        "SELECT * FROM team_seasons", dbm.engine.raw_connection()
    )

    # Excluce teams already in the table
    df_teams = df_teams.merge(
        existing_team_seasons,
        on=["web_id", "season", "team_id"],
        how="left",
        indicator=True,
    )
    df_teams = df_teams.loc[
        df_teams["_merge"] == "left_only", ["web_id", "name", "team_id", "season"]
    ]
    if df_teams.empty:
        return None
    dbm.insert("team_seasons", df_teams.to_dict(orient="records"))
    return None


def load_to_db(dbm, df_teams, season=25):
    existing_teams = get_existing_teams(dbm)
    new_teams = df_teams[~df_teams["name"].isin(existing_teams)]
    if not new_teams.empty:
        dbm.insert("teams", new_teams.to_dict(orient="records"))
    _ = add_new_team_seasons(dbm, df_teams, season)
    return None


def load_from_scrape(dbm, season=25):
    df_teams = load_scraped_teams()  # the team season data
    _ = load_to_db(dbm, df_teams, season)
    return None


def load_from_file(dbm, season=24):
    df_teams = pd.read_csv(RAW / f"team_ids_{season}.csv")
    df_teams = df_teams[["id", "name"]].rename(columns={"id": "web_id"})
    df_teams["season"] = season
    df_teams = clean_teams(df_teams)

    _ = load_to_db(dbm, df_teams, season)
    return None


if __name__ == "__main__":
    dbm = DBManager(db_path="/Users/toby/Dev/lionel/data/fpl_test.db")
    load_from_file(dbm, season=24)
    load_from_scrape(dbm, season=25)
    print("Teams loaded")
