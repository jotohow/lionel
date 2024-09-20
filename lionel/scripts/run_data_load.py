import json
import datetime as dt
from tqdm import tqdm
import sys
from pathlib import Path

import lionel.scripts.dl_db_upload as dl_db_upload
import lionel.scripts.dl_init_db as dl_init_db
from lionel.constants import RAW, DATA
from lionel.utils import setup_logger
from scrapl.fpl.runner import run_scrapers


logger = setup_logger(__name__)


def run(season, elements=[]):
    # Init the database if needed
    if not Path(DATA / "fpl.db").exists():
        dl_init_db.main()

    # Run scrapers if needed
    today = dt.datetime.today().strftime("%Y%m%d")
    p = RAW / f"scraped_data_{today}.json"
    if not p.exists():
        data = run_scrapers(elements)
        json.dump(data, open(p, "w"))

    dl_db_upload.run(season=season)
    return True


if __name__ == "__main__":

    try:
        season = int(sys.argv[1])
    except IndexError:
        season = 25
    run(season)
