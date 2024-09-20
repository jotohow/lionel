import sys
import datetime as dt
from lionel.scripts.process_train_data import get_train
from lionel.db.connector import DBManager
from lionel.constants import DATA
from lionel.model.hierarchical import FPLPointsModel


def run(next_gw, season=25):
    dbm = DBManager(DATA / "fpl.db")
    data = get_train(dbm, season, next_gw)
    fplm = FPLPointsModel()
    fplm.fit(data, data.points)
    today = dt.datetime.today().strftime("%Y%m%d")
    fplm.save(DATA / f"analysis/hm_{today}.nc")
    return True


if __name__ == "__main__":
    run(*[int(x) for x in sys.argv[1:]])
