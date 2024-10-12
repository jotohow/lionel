import datetime as dt
import json

import pandas as pd

from lionel.constants import RAW
from lionel.db.connector import DBManager


def load_scraped_gameweeks():
    today = dt.datetime.today().strftime("%Y%m%d")
    p = RAW / f"scraped_data_{today}.json"
    data = json.load(open(p, "r"))
    _ = {int(k): v for k, v in data["gw_deadlines"].items()}
    df = pd.DataFrame.from_dict(_, orient="index", columns=["deadline"])
    df["deadline"] = pd.to_datetime(df["deadline"])
    df = df.reset_index().rename(columns={"index": "gameweek"})
    return df


def run(dbm, season):
    df = load_scraped_gameweeks()
    df["season"] = season
    dbm.delete_rows("gameweeks", season)
    dbm.insert("gameweeks", df.to_dict(orient="records"))
    return None


if __name__ == "__main__":
    dbm = DBManager(db_path="/Users/toby/Dev/lionel/data/fpl_test.db")
    run(dbm, season=25)
    print("Gameweeks loaded")
