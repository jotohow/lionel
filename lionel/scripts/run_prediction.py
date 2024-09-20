import pandas as pd
import numpy as np

from lionel.model.hierarchical import FPLPointsModel

# TODO: How can this be made more general? it is tightly coupled
# to my data shape... think that's ok for now as users can replicate it with
# these scripts...

# originally in process_train_data (but this isn't related to training so)


def build_future_fixture_df(dbm, season, next_gw):

    # Get fixtures for all gameweeks after the current one
    df_fix = pd.read_sql("fixtures", dbm.engine)
    df_fix = df_fix[(df_fix["season"] == season) & (df_fix["gameweek"] >= next_gw)]

    # Get team_names from the DB
    df_teams = pd.read_sql("teams_season", dbm.engine)
    df_teams["season"] = df_teams["team_season_id"].astype(str).str[:2].astype(int)

    # Add team names to fixture list
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
    return df_fix[["season", "gameweek", "home_team", "away_team"]]


def build_recent_games_df(df, season, next_gw):

    # Filter games from the last 3 gameweeks
    df_recent = df[
        (
            (df["gameweek"] >= next_gw - 3) & (df["season"] == season)
            | (df["gameweek"] >= 38 - (3 - next_gw)) & (df["season"] == season - 1)
        )
    ]

    # Get avg minutes and most recent game for each player
    df_recent["minutes"] = (
        df_recent.groupby("player")["minutes"].transform("mean").values
    )
    df_recent = (
        df_recent.sort_values(by=["season", "gameweek"]).groupby("player").tail(1)
    )

    # Clean the dataframe
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
    return df_recent


def build_pred_df(df_recent, df_fix):

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


# def build_pred_dataset(dbm, df, season, next_gw):

#     # Get fixtures for all gameweeks after the current one
#     df_fix = pd.read_sql("fixtures", dbm.engine)
#     df_fix = df_fix[(df_fix["season"] == season) & (df_fix["gameweek"] >= next_gw)]

#     # Get team_names from the DB
#     df_teams = pd.read_sql("teams_season", dbm.engine) # shouldn't be a single
#     df_teams["season"] = df_teams["team_season_id"].astype(str).str[:2].astype(int)

#     # Add team names to fixture list
#     df_fix = df_fix.merge(
#         df_teams[["team_season_id", "team_name"]],
#         left_on="team_a_season_id",
#         right_on="team_season_id",
#         suffixes=("_a", "_b"),
#         how="left",
#     ).rename(columns={"team_name": "away_team"})
#     df_fix = df_fix.merge(
#         df_teams[["team_season_id", "team_name"]],
#         left_on="team_h_season_id",
#         right_on="team_season_id",
#         suffixes=("_a", "_b"),
#         how="left",
#     ).rename(columns={"team_name": "home_team"})
#     df_fix = df_fix[["season", "gameweek", "home_team", "away_team"]]

#     # Get average minutes for players from the last 3 gameweeks
#     df_recent = df[
#         (
#             (df["gameweek"] >= next_gw - 3) & (df["season"] == season)
#             | (df["gameweek"] >= 38 - (3 - next_gw)) & (df["season"] == season - 1)
#         )
#     ]
#     df_recent["minutes"] = (
#         df_recent.groupby("player")["minutes"].transform("mean").values
#     )
#     df_recent = (
#         df_recent.sort_values(by=["season", "gameweek"]).groupby("player").tail(1)
#     )
#     df_recent = df_recent.rename(
#         columns={
#             "team_name_a": "away_team",
#             "team_name_h": "home_team",
#             "team_a_score": "away_goals",
#             "team_h_score": "home_goals",
#             "was_home": "is_home",
#             "total_points": "points",
#         }
#     )
#     df_recent[
#         [
#             "no_contribution",
#             "home_goals",
#             "away_goals",
#             "goals_scored",
#             "assists",
#             "points",
#         ]
#     ] = np.nan
#     df_recent = df_recent[FPLPointsModel.EXPECTED_COLUMNS + ["points", "value"]]
#     df_recent["team_name"] = np.where(
#         df_recent["is_home"], df_recent["home_team"], df_recent["away_team"]
#     )
#     df_recent = df_recent.drop(columns=["home_team", "away_team", "gameweek", "season"])

#     # Create dataset of all players with all upcoming fixtures
#     df_fix_h = df_fix.merge(
#         df_recent, left_on="home_team", right_on="team_name", how="inner"
#     )
#     df_fix_a = df_fix.merge(
#         df_recent, left_on="away_team", right_on="team_name", how="inner"
#     )
#     df_pred = pd.concat([df_fix_h, df_fix_a], axis=0)
#     df_pred["is_home"] = np.where(
#         df_pred["home_team"] == df_pred["team_name"], True, False
#     )
#     df_pred = df_pred[
#         FPLPointsModel.EXPECTED_COLUMNS + ["points", "value"]
#     ].reset_index(drop=True)

#     return df_pred


def get_predictions(df_pred, next_gw, fplm):
    points_pred = fplm.predict(df_pred, predictions=True)
    df_pred["points_pred"] = points_pred
    df_pred = df_pred.loc[df_pred.gameweek <= next_gw + 5]
    df_pred["team_name"] = np.where(
        df_pred.is_home, df_pred.home_team, df_pred.away_team
    )
    df_pred = (
        df_pred.groupby(["player", "team_name", "position", "value"])
        .agg(
            mean_points_pred=("points_pred", "mean"),
            next_points_pred=("points_pred", "first"),
        )
        .reset_index()
    )
    df_pred[["xv", "xi", "captain"]] = 0
    return df_pred


def run(season, next_gw, dbm, fplm):
    df = fplm.X
    df_pred = build_pred_df(dbm, df, season, next_gw)
    return get_predictions(df_pred, next_gw, fplm)
