import pandas as pd
from lionel.data_load.constants import (
    BASE_URL,
    SEASON_MAP,
)
from lionel.utils import setup_logger
from pandas.errors import IntCastingNaNError

logger = setup_logger(__name__)

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
    validate_fixtures(df_fixtures)
    return df_fixtures


def validate_fixtures(df_fixtures):
    missing_cols = [col for col in NEEDED_COLS.keys() if col not in df_fixtures.columns]
    assert not missing_cols, f"Missing columns in fixtures: {missing_cols}"

    for col, dtype in NEEDED_COLS.items():
        # assert (
        #     df_fixtures[col].dtype == dtype
        # ), f"Invalid dtype for {col}. Expected {dtype}, got {df_fixtures[col].dtype}"

        if df_fixtures[col].dtype == dtype:
            continue
        else:
            logger.error(
                f"Invalid dtype for {col}. Expected {dtype}, got {df_fixtures[col].dtype}."
                "Attempting to convert..."
            )
            try:
                df_fixtures[col] = df_fixtures[col].astype(dtype)
            except IntCastingNaNError as e:
                logger.warning(
                    f"Error converting {col}: {e}. Shouldn't be problematic."
                )
            except Exception as e:
                logger.error(f"Error converting {col}: {e}")
                raise e

    return None
