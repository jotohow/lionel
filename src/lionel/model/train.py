import datetime as dt
import itertools

import numpy as np
import pandas as pd

from lionel.constants import ANALYSIS, DATA

# import lionel.data_load.storage_handler as storage_handler
from lionel.db.connector import DBManager
from lionel.model.hierarchical import FPLPointsModel


def run(dbm, next_gw, season, sampler_config=None):
    """
    Fit the hierarchical model.

    Args:
        dbm (object): The database manager object.
        next_gw (int): The next gameweek.
        season (int): The season.
        sampler_config (dict, optional): The configuration for the sampler. Defaults to None.

    Returns:
        bool: True if the model is successfully trained and saved.
    """

    q = f"""
    SELECT * FROM training 
    WHERE 
        (season = {season} AND gameweek < {next_gw}) 
        OR (season = {season-1} AND gameweek > {next_gw});
    """
    data = pd.read_sql(q, dbm.engine.raw_connection())

    fplm = FPLPointsModel(sampler_config=sampler_config)
    # fplm.build_model(data, data.points)
    fplm.fit(data, data.points)
    today = dt.datetime.today().strftime("%Y%m%d")
    fplm.save(ANALYSIS / f"hm_{today}.nc")
    return True


if __name__ == "__main__":
    dbm = DBManager(db_path=DATA / "lionel.db")
    run(dbm, next_gw=9, season=25)
