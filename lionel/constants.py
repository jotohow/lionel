import datetime as dt
from pathlib import Path

BASE = Path(__file__).parents[2]
DATA = BASE / "data"
RAW = DATA / "raw"
PROCESSED = DATA / "processed"
CLEANED = DATA / "cleaned"
ANALYSIS = DATA / "analysis"

TODAY = dt.datetime.today()
# TODAY = dt.datetime(2024, 10, 25)
print(f"Running for {TODAY}")

SEASON_MAP = {
    25: "2024-25",
    24: "2023-24",
    23: "2022-23",
    22: "2021-22",
    21: "2020-21",
    20: "2019-20",
    19: "2018-19",
}

SEASON_DATES = {
    25: {"start": "2024-08-09", "end": "2025-05-17"},
    24: {"start": "2023-08-11", "end": "2024-05-19"},
}
