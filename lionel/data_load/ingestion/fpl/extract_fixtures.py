import pandas as pd
from pathlib import Path
from lionel.data_load.constants import (
    BASE,
    DATA,
    RAW,
    BASE_URL,
    SEASON_MAP,
)

NEEDED_COLS = {
    "code": "int64",
    "event": "int64",
    "finished": "bool",
    "finished_provisional": "bool",
    "id": "int64",
    "kickoff_time": "object",
    "minutes": "int64",
    "provisional_start_time": "bool",
    "started": "bool",
    "team_a": "int64",
    "team_a_score": "int64",
    "team_h": "int64",
    "team_h_score": "int64",
    "stats": "object",
    "team_h_difficulty": "int64",
    "team_a_difficulty": "int64",
    "pulse_id": "int64",
}


def get_fixtures(season):
    df_fixtures = pd.read_csv(f"{BASE_URL}/{SEASON_MAP[season]}/fixtures.csv")
    return df_fixtures


def validate_fixtures(df_fixtures):
    missing_cols = [col for col in NEEDED_COLS.keys() if col not in df_fixtures.columns]
    assert not missing_cols, f"Missing columns in fixtures: {missing_cols}"

    for col, dtype in NEEDED_COLS.items():
        assert df_fixtures[col].dtype == dtype, f"Invalid dtype for {col}"
    return None


def update_local_fixtures(season):
    df_fixtures = get_fixtures(season)
    validate_fixtures(df_fixtures)
    df_fixtures.to_csv(RAW / f"fixtures_{season}.csv", index=False)
    return None


if __name__ == "__main__":
    update_local_fixtures(24)
    update_local_fixtures(23)
