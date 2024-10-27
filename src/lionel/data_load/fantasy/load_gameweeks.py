import datetime as dt
import json

import pandas as pd

import lionel.scripts.data_load.scrape as scrape
from lionel.constants import RAW, TODAY
from lionel.db.connector import DBManager


def load_scraped_gameweeks():
    today = TODAY.strftime("%Y%m%d")
    p = RAW / f"scraped_data_{today}.json"
    data = json.load(open(p, "r"))
    _ = {int(k): v for k, v in data["gw_deadlines"].items()}
    df = pd.DataFrame.from_dict(_, orient="index", columns=["deadline"])
    df["deadline"] = pd.to_datetime(df["deadline"])
    df = df.reset_index().rename(columns={"index": "gameweek"})
    return df


# TODO: Adjust to use the
def load_from_scrape(dbm, season):

    df = load_scraped_gameweeks()
    df["season"] = season
    dbm.delete_rows_by_season("gameweeks", season)
    dbm.insert("gameweeks", df.to_dict(orient="records"))
    return None


def load_historic(dbm, season):
    # Don't care about the deadlines for previous ones
    df = pd.DataFrame(data={"gameweek": list(range(1, 39)), "season": season})
    dbm.insert("gameweeks", df.to_dict(orient="records"))
    return None


if __name__ == "__main__":
    dbm = DBManager(db_path="/Users/toby/Dev/lionel/data/fpl_test.db")
    load_historic(dbm, season=24)
    load_from_scrape(dbm, season=25)
    print("Gameweeks loaded")
