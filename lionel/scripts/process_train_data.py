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


def get_pred_dataset(dbm, df, season, next_gw):
    df_fix = pd.read_sql("fixtures", dbm.engine)
    df_fix = df_fix[(df_fix["season"] == season) & (df_fix["gameweek"] >= next_gw)]

    df_teams = pd.read_sql("teams_season", dbm.engine)
    df_teams["season"] = df_teams["team_season_id"].astype(str).str[2].astype(int)

    df_fix = df_fix.merge(
        df_teams[["team_season_id", "team_name"]],
        left_on="team_a_season_id",
        right_on="team_season_id",
        suffixes=("_a", "_b"),
        how="left",
    ).rename(columns={"team_name": "away_team"})
    df_fix = df_fix.merge(
        df_teams[["team_season_id", "team_name"]],
        left_on="team_h_season_id",
        right_on="team_season_id",
        suffixes=("_a", "_b"),
        how="left",
    ).rename(columns={"team_name": "home_team"})

    df_fix = df_fix[["season", "gameweek", "home_team", "away_team"]]

    df_recent = df[
        (
            (df["gameweek"] >= next_gw - 3) & (df["season"] == season)
            | (df["gameweek"] >= 38 - (3 - next_gw)) & (df["season"] == season - 1)
        )
    ]

    df_recent["minutes"] = (
        df_recent.groupby("player")["minutes"].transform("mean").values
    )
    df_recent = (
        df_recent.sort_values(by=["season", "gameweek"]).groupby("player").tail(1)
    )

    df_recent = df_recent.rename(
        columns={
            "team_name_a": "away_team",
            "team_name_h": "home_team",
            "team_a_score": "away_goals",
            "team_h_score": "home_goals",
            "was_home": "is_home",
            "total_points": "points",
        }
    )
    df_recent[
        [
            "no_contribution",
            "home_goals",
            "away_goals",
            "goals_scored",
            "assists",
            "points",
        ]
    ] = np.nan
    df_recent = df_recent[FPLPointsModel.EXPECTED_COLUMNS + ["points", "value"]]
    df_recent["team_name"] = np.where(
        df_recent["is_home"], df_recent["home_team"], df_recent["away_team"]
    )
    df_recent = df_recent.drop(columns=["home_team", "away_team", "gameweek", "season"])

    df_fix_h = df_fix.merge(
        df_recent, left_on="home_team", right_on="team_name", how="inner"
    )
    df_fix_a = df_fix.merge(
        df_recent, left_on="away_team", right_on="team_name", how="inner"
    )
    df_pred = pd.concat([df_fix_h, df_fix_a], axis=0)
    df_pred["is_home"] = np.where(
        df_pred["home_team"] == df_pred["team_name"], True, False
    )
    df_pred = df_pred[
        FPLPointsModel.EXPECTED_COLUMNS + ["points", "value"]
    ].reset_index(drop=True)

    return df_pred


# if __name__ == "__main__":
# sh = storage_handler.StorageHandler(local=True)
# season = 24
# next_gw = 27
# # horizon = 1
# df = run(sh, next_gw, season)
# sh.store(df, f"analysis/train_{next_gw}_{season}.csv", index=False)
# print(df_train.head())
# print(df_forecast.head())
