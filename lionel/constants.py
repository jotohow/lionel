from pathlib import Path

BASE = Path(__file__).parents[1]
DATA = BASE / "data"
RAW = DATA / "raw"
PROCESSED = DATA / "processed"
CLEANED = DATA / "cleaned"
ANALYSIS = DATA / "analysis"

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
