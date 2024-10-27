import datetime as dt
import json

import pandas as pd

from lionel.constants import RAW, TODAY
from lionel.data_load.fantasy.config import DataLoadConfig as Config
from lionel.db.connector import DBManager

today = TODAY.strftime("%Y%m%d")


def load_scraped_stats():

    p = RAW / f"scraped_data_{today}.json"
    data = json.load(open(p, "r"))

    _ = sum([v for k, v in data.items() if "player_stats" in k], [])

    df = pd.DataFrame(_)
    return df[Config.STATS_KEEP_COLS].rename(columns={"was_home": "is_home"})


def load_stats_from_file(season):
    df = pd.read_csv(RAW / f"player_stats_{season}.csv")
    return df[Config.STATS_KEEP_COLS].rename(columns={"was_home": "is_home"})


def process_stats(dbm, df, season):
    q = (
        "SELECT id AS fixture_id, "
        f"fixture_season_id, gameweek_id FROM fixtures WHERE season = {season}"
    )
    fixtures = pd.read_sql(q, dbm.engine.raw_connection())
    df = df.merge(
        fixtures,
        left_on="fixture",
        right_on="fixture_season_id",
        how="left",
        indicator=True,
    )
    assert df._merge.unique() == ["both"]
    df = df.drop(columns=["fixture_season_id", "fixture", "_merge"])

    q = f"SELECT player_id, web_id FROM player_seasons WHERE season = {season}"
    players = pd.read_sql(q, dbm.engine.raw_connection())

    df = df.merge(
        players, left_on="element", right_on="web_id", how="left", indicator=True
    )
    assert df._merge.unique() == ["both"]
    df = df.drop(columns=["web_id", "element", "_merge"])
    df["season"] = season
    return df


def load_from_scrape(dbm, season):

    df = load_scraped_stats()
    df = process_stats(dbm, df, season)
    dbm.delete_rows("stats", season)
    dbm.insert("stats", df.to_dict(orient="records"))
    return None


def load_from_file(dbm, season):
    df = load_stats_from_file(season)
    df = process_stats(dbm, df, season)
    dbm.delete_rows("stats", season)
    dbm.insert("stats", df.to_dict(orient="records"))
    return None


def run(dbm, season):

    q = (
        "SELECT id AS fixture_id, "
        f"fixture_season_id, gameweek_id FROM fixtures WHERE season = {season}"
    )
    fixtures = pd.read_sql(q, dbm.engine)

    df = load_scraped_stats()

    df = df.merge(
        fixtures,
        left_on="fixture",
        right_on="fixture_season_id",
        how="left",
        indicator=True,
    )
    assert df._merge.unique() == ["both"]
    df = df.drop(columns=["fixture_season_id", "fixture", "_merge"])

    q = f"SELECT player_id, web_id FROM player_seasons WHERE season = {season}"
    players = pd.read_sql(
        q,
        dbm.engine,
    )

    df = df.merge(
        players, left_on="element", right_on="web_id", how="left", indicator=True
    )
    assert df._merge.unique() == ["both"]
    df = df.drop(columns=["web_id", "element", "_merge"])
    df["season"] = season

    dbm.delete_rows("stats", season)
    dbm.insert("stats", df.to_dict(orient="records"))


if __name__ == "__main__":
    dbm = DBManager(db_path="/Users/toby/Dev/lionel/data/fpl_test.db")
    load_from_file(dbm, 24)
    load_from_scrape(dbm, 25)
    print("Player stats loaded")
