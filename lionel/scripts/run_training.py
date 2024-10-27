import datetime as dt
import sys

from lionel.constants import ANALYSIS, DATA
from lionel.db.connector import DBManager
from lionel.model.hierarchical import FPLPointsModel
from lionel.scripts.train_process_data import get_train


def run(dbm, next_gw, season, sampler_config=None):
    """
    Run the training process for the FPLPointsModel.

    Args:
        dbm (DatabaseManager): The database manager object.
        next_gw (int): The next game week.
        season (str): The season identifier.
        sampler_config (dict, optional): Configuration for the sampler. Defaults to None.

    Returns:
        bool: True if the training process is successful.
    """
    data = get_train(dbm, season, next_gw)

    fplm = FPLPointsModel(sampler_config=sampler_config)
    fplm.build_model(data, data.points)
    fplm.fit(data, data.points)
    today = dt.datetime.today().strftime("%Y%m%d")
    fplm.save(ANALYSIS / f"hm_{today}.nc")
    return True


if __name__ == "__main__":
    dbm = DBManager(DATA / "fpl.db")
    sampler_config = {
        "draws": 1000,
        "tune": 200,
        "chains": 4,
    }
    next_gw, season = [int(x) for x in sys.argv[1:]]
    run(dbm, next_gw, season, sampler_config)

    # python -m lionel.scripts.run_training 9 25
