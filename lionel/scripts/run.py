import sys

from lionel.data_load.db.connector import DBManager
from lionel.model.hierarchical import FPLPointsModel
from lionel.data_load.constants import DATA

import lionel.scripts.run_data_load as run_data_load
import lionel.scripts.run_team as run_team


def get_player_strength_data(fplm):
    pass


def run(season, next_gw):

    # Run the dataload
    _ = run_data_load.run(season)

    # Load model
    fplm = FPLPointsModel.load(DATA / "analysis/hm_02.nc")
    dbm = DBManager(DATA / "fpl.db")

    # Put inference data in DB
    df_theta = fplm.summarise_players()
    df_theta.to_sql("player_inference", dbm.engine, if_exists="replace", index=False)

    df_beta = fplm.summarise_teams()
    df_beta.to_sql("team_inference", dbm.engine, if_exists="replace", index=False)

    # Make selections
    df_selection, df_scoreline, df_next = run_team.run(season, next_gw, dbm, fplm)

    # Send it back to DB
    table = dbm.tables["selections"]
    dele = table.delete().where(
        table.c.season == season and table.c.gameweek == next_gw
    )
    with dbm.engine.connect() as conn:
        conn.execute(dele)
        conn.commit()
    df_selection.to_sql("selections", dbm.engine, if_exists="append", index=False)

    # Also get out scoreline predictions
    df_scoreline.to_sql("scorelines", dbm.engine, if_exists="replace", index=False)
    df_next.to_sql("next_games", dbm.engine, if_exists="replace", index=False)


if __name__ == "__main__":
    run(*[int(x) for x in sys.argv[1:]])
