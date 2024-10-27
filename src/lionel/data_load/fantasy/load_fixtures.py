import datetime as dt

import pandas as pd

import lionel.data_load.fantasy.scrape as scrape
from lionel.constants import RAW
from lionel.data_load.fantasy.config import DataLoadConfig as Config
from lionel.db.connector import DBManager


def clean_fixtures(df):
    df["kickoff_time"] = pd.to_datetime(df["kickoff_time"]).dt.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    df = df.rename(columns=Config.FIXTURE_COL_MAP)
    return df[Config.FIXTURE_RETURN_COLS]


def process_fixtures(dbm, df, season):

    # Clean col names etc
    df["kickoff_time"] = pd.to_datetime(df["kickoff_time"]).dt.strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    df = df.rename(columns=Config.FIXTURE_COL_MAP)
    df = df[Config.FIXTURE_RETURN_COLS]
    df["season"] = season

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
        f"SELECT id, gameweek FROM gameweeks WHERE season = {season}",
        dbm.engine.raw_connection(),
    )

    df = df.merge(gameweeks, left_on="gameweek", right_on="gameweek").rename(
        columns=Config.GW_COL_MAP
    )
    return df


def load_from_scrape(dbm, season):
    k = "fixtures"
    data = scrape.read_from_scrape([k])[k]
    df = pd.DataFrame(data)
    df = process_fixtures(dbm, df, season)

    # add to the db
    dbm.delete_rows("fixtures", season)
    dbm.insert("fixtures", df.to_dict(orient="records"))

    return None


def load_from_file(dbm, season):
    df = pd.read_csv(RAW / f"fixtures_{season}.csv")
    df = clean_fixtures(df)

    df = process_fixtures(dbm, df, season)
    dbm.delete_rows("fixtures", season)
    dbm.insert("fixtures", df.to_dict(orient="records"))
    return None


if __name__ == "__main__":
    dbm = DBManager(db_path="/Users/toby/Dev/lionel/data/fpl_test.db")
    # load_from_file(dbm, season=24)
    load_from_scrape(dbm, season=25)
    print("Fixtures loaded")
