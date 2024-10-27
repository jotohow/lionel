import datetime as dt
import json

from scrapl.fpl.runner import run_scrapers

from lionel.constants import RAW, TODAY

today = TODAY.strftime("%Y%m%d")
PATH = RAW / f"scraped_data_{today}.json"
print(PATH)


def scrape(elements=[], date=today):
    data = run_scrapers(elements)
    json.dump(data, open(str(PATH).format(date=date), "w"))


def read_from_scrape(keys, path=PATH):
    """Load a subset of the data from the scrape"""
    data = json.load(open(path, "r"))
    return {k: data[k] for k in keys}
