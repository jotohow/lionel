import sys
from lionel.selector import run_selection


def run(df_pred_next_5, season, next_gw, dbm):
    """
    Runs the team selection for the next gameweek.

    Args:
        df_pred_next_5 (DataFrame): The DataFrame containing the player data with predicted points
        season (str): The season for which the selection is being made.
        next_gw (int): The next gameweek number.
        dbm (DatabaseManager): The database manager object.

    Returns:
        DataFrame: The DataFrame containing the selected data for the next gameweek.
    """

    df_selection = run_selection(df_pred_next_5)
    df_selection["gameweek"] = next_gw

    # Should this be here? probably doing too much in this function if so...
    try:
        table = dbm.tables["selections"]
        dele = table.delete().where(
            table.c.season == season and table.c.gameweek == next_gw
        )
        with dbm.engine.connect() as conn:
            conn.execute(dele)
            conn.commit()
    except KeyError:
        pass
    df_selection["season"] = season
    df_selection["gameweek"] = next_gw
    df_selection.to_sql("selections", dbm.engine, if_exists="append", index=False)

    return df_selection  # ,  df_scoreline, df_next


if __name__ == "__main__":
    from lionel.db.connector import DBManager
    from lionel.model.hierarchical import FPLPointsModel
    from lionel.constants import ANALYSIS

    dbm = DBManager()
    fplm = FPLPointsModel.load(ANALYSIS / "hm_02.nc")
    run(*[int(x) for x in sys.argv[1:]], dbm, fplm)
