import pandas as pd
from lionel.data_load.constants import (
    BASE,
    DATA,
    RAW,
    BASE_URL,
    SEASON_MAP,
)

NEEDED_COLS = {
    "GW": ("int64", "int64"),
    "assists": ("int64", "int64"),
    "bps": ("int64", "int64"),
    "creativity": ("float64", "float64"),
    "element": ("int64", "int64"),
    "goals_scored": ("int64", "int64"),
    "ict_index": ("float64", "float64"),
    "influence": ("float64", "float64"),
    "minutes": ("int64", "int64"),
    "name": ("object", "object"),
    "opponent_team": ("int64", "int64"),
    "position": ("object", "object"),
    "season": ("int64", "int64"),
    "selected": ("int64", "int64"),
    "team_a_score": ("int64", "int64"),
    "team_h_score": ("int64", "int64"),
    "team": ("object", "object"),
    "threat": ("float64", "float64"),
    "total_points": ("int64", "int64"),
    "transfers_balance": ("int64", "int64"),
    "value": ("int64", "int64"),
    "was_home": ("bool", "bool"),
    "kickoff_time": ("object", "datetime64[D]"),
}

NAME_MAP = {
    "team_name": {
        "Man City": "Manchester City",
        "Man Utd": "Manchester Utd",
        "Spurs": "Tottenham",
        "Nott'm Forest": "Nottingham",
    },
    "name": {
        "Son Heung-min": "Heung-Min Son",
        "João Cancelo": "João Pedro Cavaco Cancelo",
        "Emerson Leite de Souza Junior": "Emerson Aparecido Leite de Souza Junior",
    },
}

COL_RENAME = {"team": "team_name", "GW": "gameweek", "kickoff_time": "game_date"}


def validate_gw_stats(df_gw):
    missing_cols = [col for col in NEEDED_COLS.keys() if col not in df_gw.columns]
    assert not missing_cols, f"Missing columns in gw stats: {missing_cols}"

    for col, dtype in NEEDED_COLS.items():
        assert df_gw[col].dtype == dtype[0], f"Invalid dtype for {col}"
    return None


def update_dtypes(df_gw):
    for col, dtype in NEEDED_COLS.items():
        if dtype[0] == dtype[1]:
            continue
        elif dtype[0] != dtype[1] and dtype[1] == "datetime64[D]":
            df_gw[col] = pd.to_datetime(df_gw[col])
        else:
            df_gw[col] = df_gw[col].astype(dtype[1])
    return df_gw


def get_gw_stats(season):
    df_gw = pd.read_csv(f"{BASE_URL}/{SEASON_MAP[season]}/gws/merged_gw.csv")
    df_gw["season"] = season
    df_gw = df_gw[NEEDED_COLS.keys()]
    validate_gw_stats(df_gw)
    return df_gw


def update_gw_stats(df_gw_existing, df_gw_new, season):
    df_gw_existing = df_gw_existing[df_gw_existing["season"] != season]
    df_gw_new = pd.concat([df_gw_existing, df_gw_new])
    return df_gw_new


def clean_gw_stats(df_gw):
    df_gw = df_gw.rename(columns=COL_RENAME)
    df_gw = df_gw.replace(NAME_MAP)
    return df_gw


def update_local_csv(season):
    df_gw_new = get_gw_stats(season)

    try:
        df_gw_existing = pd.read_csv(
            RAW / "gw_stats.csv",
        )
        df_gw_existing = update_dtypes(df_gw_existing)
        df_gw_new = update_gw_stats(df_gw_existing, df_gw_new, season)
    except FileNotFoundError:
        pass
    df_gw_new.to_csv(RAW / "gw_stats.csv", index=False)
    return df_gw_new


def run_update(season):
    update_local_csv(season)
    return None


if __name__ == "__main__":
    run_update(24)
