import pandas as pd
import numpy as np
import datetime as dt
import itertools

# import lionel.data_load.storage_handler as storage_handler
from lionel.db.connector import DBManager
from lionel.model.hierarchical import FPLPointsModel
from lionel.db.connector import DBManager
from lionel.constants import DATA
from typing import Dict, List, Optional, Tuple, Union
from sqlalchemy.orm import sessionmaker


def get_train(dbm: DBManager, season: int, next_gw: int) -> pd.DataFrame:
    """
    Retrieves training data from the database for a given season and next game week.

    Args:
        dbm (DBManager): The database manager object.
        season (int): The season for which to retrieve the data.
        next_gw (int): The next game week for which to retrieve the data.

    Returns:
        pd.DataFrame: The training data as a pandas DataFrame.
    """

    dbm = DBManager(DATA / "fpl.db")
    Session = sessionmaker(bind=dbm.engine)

    with Session() as session:
        query = (
            session.query(
                dbm.tables["player_stats"],
                dbm.tables["player_seasons"].columns["player_name", "position"],
                dbm.tables["fixtures"].columns["team_a_season_id", "team_h_season_id"],
                dbm.tables["teams_season"].columns["team_name"].label("team_name_a"),
                dbm.tables["fixtures"].columns["team_a_score"],
            )
            .join(
                dbm.tables["player_seasons"],
                dbm.tables["player_stats"].c.player_season_id
                == dbm.tables["player_seasons"].c.player_season_id,
            )
            .join(
                dbm.tables["fixtures"],
                dbm.tables["player_stats"].c.fixture_season_id
                == dbm.tables["fixtures"].c.fixture_season_id,
            )
            .join(
                dbm.tables["teams_season"],
                dbm.tables["fixtures"].c.team_a_season_id
                == dbm.tables["teams_season"].c.team_season_id,
            )
            .all()
        )
        q2 = (
            session.query(
                dbm.tables["player_stats"].c.player_stat_id,
                dbm.tables["fixtures"].columns["team_h_season_id"],
                dbm.tables["teams_season"].columns["team_name"].label("team_name_h"),
                dbm.tables["fixtures"].columns["team_h_score"],
            )
            .join(
                dbm.tables["fixtures"],
                dbm.tables["player_stats"].c.fixture_season_id
                == dbm.tables["fixtures"].c.fixture_season_id,
            )
            .join(
                dbm.tables["teams_season"],
                dbm.tables["fixtures"].c.team_h_season_id
                == dbm.tables["teams_season"].c.team_season_id,
            )
            .all()
        )

    df_2 = pd.DataFrame(query)
    df_3 = pd.DataFrame(q2)
    df = pd.concat([df_2, df_3], axis=1)
    df = df.rename(
        columns={
            "was_home": "is_home",
            "team_name_h": "home_team",
            "team_name_a": "away_team",
            "team_h_score": "home_goals",
            "team_a_score": "away_goals",
            "total_points": "points",
        }
    )

    # If player name changes - replace it with the most recent
    df["player_name"] = (
        df.sort_values(["season", "gameweek"], ascending=[True, True])
        .groupby("player_id")["player_name"]
        .transform("first")
    )
    df["player"] = df["player_id"].astype(str) + "_" + df["player_name"]
    df["no_contribution"] = np.where(
        df.is_home,
        df.home_goals - df.assists - df.goals_scored,
        df.away_goals - df.assists - df.goals_scored,
    )
    df = df[FPLPointsModel.EXPECTED_COLUMNS + ["points", "value"]].reset_index(
        drop=True
    )

    return df
