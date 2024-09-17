import sys
from lionel.data_load.db.connector import DBManager
from lionel.model.hierarchical import FPLPointsModel
from lionel.data_load.constants import DATA
import lionel.data_load.run
import lionel.model.run
import lionel.team.run


def run(season, next_gw):

    # Run the dataload
    _ = lionel.data_load.run.run(season)

    # Load model
    fplm = FPLPointsModel.load(DATA / "analysis/hm_02.nc")

    # Make selections
    dbm = DBManager(DATA / "fpl.db")
    df_selection = lionel.team.run.run(season, next_gw, dbm, fplm)

    # Send it back to DB
    table = dbm.tables["selections"]
    dele = table.delete().where(
        table.c.season == season and table.c.gameweek == next_gw
    )
    with dbm.engine.connect() as conn:
        conn.execute(dele)
        conn.commit()
    df_selection.to_sql("selections", dbm.engine, if_exists="append", index=False)


if __name__ == "__main__":
    run(*[int(x) for x in sys.argv[1:]])
