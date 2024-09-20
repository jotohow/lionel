import sqlalchemy as sa
from sqlalchemy import create_engine
import time

import lionel.db.schema as schema
from lionel.constants import DATA, RAW
from lionel.scripts.dl_add_24_data import add_24_data
from lionel.db.connector import DBManager
from lionel.utils import setup_logger

logger = setup_logger(__name__)


def main():
    t = time.time()
    engine = create_engine(f"sqlite:///{str(DATA)}/fpl.db")
    schema.Base.metadata.create_all(engine)

    # add 24 data to db
    dbm = DBManager(DATA / "fpl.db")
    add_24_data(dbm)
    t2 = time.time()
    logger.info(f"DB initialised with 2024 data. Time taken: {round(t2-t, 2)}s")


if __name__ == "__main__":
    main()
