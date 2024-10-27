import copy
import datetime as dt
import json
from abc import ABC, abstractmethod

import pandas as pd
from scrapl.fpl.runner import run_scrapers

from lionel.constants import RAW


class BaseLoader:
    def __init__(self, dbm, data=None, date=None):
        self.database_manager = dbm
        self.data = data
        self.date = date or dt.datetime.today().strftime("%Y%m%d")

    def scrape(self, elements=[]):
        p = RAW / f"scraped_data_{self.date}.json"
        if not p.exists():
            data = run_scrapers(elements)
            self.data = data
            json.dump(data, open(p, "w"))
            return True
        return False

    def _read_from_scrape(self):
        """Method to load all the data from the scrape"""
        p = RAW / f"scraped_data_{self.date}.json"
        data = json.load(open(p, "r"))
        return data

    def read_from_file(self, path):
        self.data = pd.read_csv(path)
        return self.data

    def _delete_and_load_to_db(self, df, season, table_name):
        self.dbm.delete_rows_by_season(table_name, season)
        self.dbm.insert(table_name, df.to_dict(orient="records"))
        return None


class AbstractLoader(ABC, BaseLoader):

    @property
    @abstractmethod
    def key(self):
        pass

    # @property
    # @abstractmethod
    # def raw_file_name():
    #     pass

    def read_from_scrape(self):
        """Load a subset of the data from the scrape"""
        _data = self._read_from_scrape()
        self.data = copy.copy(_data[self.key])
        del _data
        return self.data

    def add_missing(self):
        pass

    def to_db(self):
        pass
