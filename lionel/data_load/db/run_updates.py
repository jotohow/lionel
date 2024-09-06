from lionel.data_load.constants import DATA, RAW
from lionel.data_load.db.connector import DBManager
import lionel.data_load.process.process_scraped_data as process
import datetime as dt
from pathlib import Path
import json
import pandas as pd


def load_scraped_data(date=None):
    date = date or dt.datetime.today().strftime("%Y%m%d")
    with open(RAW / f"scraped_data_{date}.json", "r") as f:
        data = json.load(f)
    return data


def main():
    dbmanager = DBManager(DATA / "fpl.db")
    data = load_scraped_data()

    # Clean each set of data
    df_teams = process.clean_teams(data)
    df_fixtures = process.clean_fixtures(data)
    df_gameweeks = process.clean_gameweeks(data)
    df_players = process.clean_players(data)
    df_player_stats = process.clean_player_stats(data)

    # Write to db
    # dbmanager.delete_rows(dbmanager.tables["teams"], 25)
    df_teams.to_sql("teams", con=dbmanager.engine, if_exists="replace", index=False)

    dbmanager.delete_rows(dbmanager.tables["fixtures"], 25)
    df_fixtures.to_sql(
        "fixtures", con=dbmanager.engine, if_exists="append", index=False
    )

    dbmanager.delete_rows(dbmanager.tables["gameweeks"], 25)
    df_gameweeks.to_sql(
        "gameweeks", con=dbmanager.engine, if_exists="append", index=False
    )

    # dbmanager.delete_rows(dbmanager.tables["players"], 25)
    df_players.to_sql("players", con=dbmanager.engine, if_exists="replace", index=False)

    dbmanager.delete_rows(dbmanager.tables["player_stats"], 25)
    df_player_stats.to_sql(
        "player_stats", con=dbmanager.engine, if_exists="append", index=False
    )
    return True


if __name__ == "__main__":
    main()
