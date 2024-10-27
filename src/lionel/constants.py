import datetime as dt
from pathlib import Path

BASE = Path(__file__).parents[2]
DATA = BASE / "data"
RAW = DATA / "raw"
PROCESSED = DATA / "processed"
CLEANED = DATA / "cleaned"
ANALYSIS = DATA / "analysis"

# TODAY = dt.datetime.today()
# TODAY = dt.datetime.today() - dt.timedelta(days=1)
TODAY = dt.datetime(2024, 10, 25)
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

TEAM_MAP = {
    "team_name": {
        "Man City": "Manchester City",
        "Man Utd": "Manchester Utd",
        "Spurs": "Tottenham",
        "Nott'm Forest": "Nottingham",
    }
}

PLAYER_MAP = {
    "full_name": {"Dominic Solanke-Mitchell": "Dominic Solanke"},
    "name": {
        "Philip": "Billing",
        "J.Ramsey": "Ramsey",
        "Cook": "L.Cook",
        "H.Traorè": "Hamed Traorè",
        "Kozłowski": "Kozlowski",
        "Sánchez": "Sanchez",
        "Andrey Santos": "Andrey",
        "Vinicius": "Vinícius",
        "N.Phillips": "Phillips",
        "M.Salah": "Salah",
        "N.Willians": "Williams",
        "P.M.Sarr": "Sarr",
        "J.Gomes": "João Gomes",
    },
}

SEASON_DATES = {
    25: {"start": "2024-08-09", "end": "2025-05-17"},
    24: {"start": "2023-08-11", "end": "2024-05-19"},
}
