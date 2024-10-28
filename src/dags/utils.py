from lionel.constants import DATA, TODAY
from lionel.db.connector import DBManager


def get_next_gw_season(dbm):
    # TODO: What if it's the end of the season?
    q = """
        SELECT gameweek, season
        FROM gameweeks 
        WHERE deadline > CURRENT_DATE 
        ORDER BY deadline ASC
        LIMIT 1;
        """
    next_gw, season = dbm.query(q)[0]
    return next_gw, season


dbm = DBManager(db_path=DATA / "lionel.db")
today = TODAY.strftime("%Y%m%d")
next_gw, season = get_next_gw_season(dbm)
