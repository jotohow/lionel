import datetime as dt
import json

import pandas as pd

from lionel.constants import PLAYER_MAP, RAW
from lionel.db.connector import DBManager


def load_scraped_unique_players():
    today = dt.datetime.today().strftime("%Y%m%d")
    p = RAW / f"scraped_data_{today}.json"
    data = json.load(open(p, "r"))
    df_players = pd.DataFrame(data["element_map"].values())
    df_players["full_name"] = df_players["first_name"] + " " + df_players["second_name"]
    df_players = df_players.rename(columns={"web_name": "name"})
    df_players = df_players.replace(PLAYER_MAP)
    df_players["position"] = df_players["element_type"].map(
        {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}
    )
    return df_players[["id", "name", "full_name", "position"]]


def get_existing_players(dbm: DBManager):

    q = "SELECT DISTINCT full_name FROM players"
    existing_players = dbm.query(q).fetchall()
    return [x[0] for x in existing_players]


def build_player_seasons(dbm, df_players, season=25):
    df_players = df_players.rename(columns={"id": "web_id"})
    players_unique = pd.read_sql("SELECT * FROM players", dbm.engine)
    df_players = df_players.merge(
        players_unique[["id", "full_name"]], on="full_name", how="left"
    )
    df_players["season"] = season
    df_players = df_players[["web_id", "id", "season", "position"]].rename(
        columns={"id": "player_id"}
    )
    assert df_players["player_id"].isnull().sum() == 0
    return df_players


def update_player_seasons(dbm, df_players, season=25):
    df_player_seasons = build_player_seasons(dbm, df_players, season)
    dbm.delete_rows("player_seasons", 25)
    dbm.insert("player_seasons", df_player_seasons.to_dict(orient="records"))
    return None


def run(dbm, season=25):

    # Load from scraped data
    df_players = load_scraped_unique_players()

    # Add new unique players
    existing_players = get_existing_players(dbm)
    new_players = df_players.loc[
        ~df_players["full_name"].isin(existing_players), ["name", "full_name"]
    ]
    dbm.insert("players", new_players.to_dict(orient="records"))

    _ = update_player_seasons(dbm, df_players, season)

    return None


if __name__ == "__main__":
    dbm = DBManager(db_path="/Users/toby/Dev/lionel/data/fpl_test.db")
    run(dbm, season=25)
    print("Players loaded")
