import datetime as dt
import json

import pandas as pd

from lionel.constants import RAW
from lionel.db.connector import DBManager


def load_scraped_fixtures():
    today = dt.datetime.today().strftime("%Y%m%d")
    p = RAW / f"scraped_data_{today}.json"
    data = json.load(open(p, "r"))
    df = pd.DataFrame(data["fixtures"])
    return df


def load_fixtures_from_file(season):
    df = pd.read_csv(RAW / f"fixtures_raw_{season}.csv")
    return df


def clean_fixtures(df):
    df["kickoff_time"] = pd.to_datetime(df["kickoff_time"]).dt.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    df = df.rename(
        columns={
            "event": "gameweek",
            "team_h": "home_id",
            "team_a": "away_id",
            "team_h_score": "home_score",
            "team_a_score": "away_score",
            "id": "fixture_season_id",
        }
    )
    return df[
        [
            "gameweek",
            "home_id",
            "away_id",
            "home_score",
            "away_score",
            "kickoff_time",
            "fixture_season_id",
        ]
    ]


def process_fixtures(dbm, df, season):
    # Get team IDs
    team_seasons = pd.read_sql(
        f"SELECT web_id, team_id FROM team_seasons WHERE season={season}",
        dbm.engine.raw_connection(),
    )
    df = (
        df.merge(team_seasons, left_on="home_id", right_on="web_id")
        .drop(columns=["web_id", "home_id"])
        .rename(columns={"team_id": "home_id"})
    )
    df = (
        df.merge(team_seasons, left_on="away_id", right_on="web_id")
        .drop(columns=["web_id", "away_id"])
        .rename(columns={"team_id": "away_id"})
    )

    # Get the gameweek IDs from the database
    gameweeks = pd.read_sql(
        "SELECT id, gameweek FROM gameweeks WHERE season = 25",
        dbm.engine.raw_connection(),
    )

    df = df.merge(gameweeks, left_on="gameweek", right_on="gameweek").rename(
        columns={"id": "gameweek_id"}
    )
    return df


def load_from_scrape(dbm, season):
    df = load_scraped_fixtures()
    df = clean_fixtures(df)
    df["season"] = season
    df = process_fixtures(dbm, df, season)
    dbm.delete_rows("fixtures", season)
    dbm.insert("fixtures", df.to_dict(orient="records"))
    return None


def load_from_file(dbm, season):
    df = pd.read_csv(RAW / f"fixtures_{season}.csv")
    df = clean_fixtures(df)
    df["season"] = season
    df = process_fixtures(dbm, df, season)
    dbm.delete_rows("fixtures", season)
    dbm.insert("fixtures", df.to_dict(orient="records"))
    return None


# def run(dbm, season):
#     df = load_scraped_fixtures()
#     df = clean_fixtures(df)
#     df["season"] = season

#     # Get team IDs
#     team_seasons = pd.read_sql(
#         f"SELECT web_id, team_id FROM team_seasons WHERE season={season}", dbm.engine
#     )
#     df = (
#         df.merge(team_seasons, left_on="home_id", right_on="web_id")
#         .drop(columns=["web_id", "home_id"])
#         .rename(columns={"team_id": "home_id"})
#     )
#     df = (
#         df.merge(team_seasons, left_on="away_id", right_on="web_id")
#         .drop(columns=["web_id", "away_id"])
#         .rename(columns={"team_id": "away_id"})
#     )

#     # Get the gameweek IDs from the database
#     gameweeks = pd.read_sql(
#         "SELECT id, gameweek FROM gameweeks WHERE season = 25", dbm.engine
#     )

#     df = df.merge(gameweeks, left_on="gameweek", right_on="gameweek").rename(
#         columns={"id": "gameweek_id"}
#     )

#     dbm.delete_rows("fixtures", season)
#     dbm.insert("fixtures", df.to_dict(orient="records"))
#     return None


if __name__ == "__main__":
    dbm = DBManager(db_path="/Users/toby/Dev/lionel/data/fpl_test.db")
    load_from_file(dbm, season=24)
    load_from_scrape(dbm, season=25)
    print("Fixtures loaded")
