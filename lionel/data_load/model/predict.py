import numpy as np
import pandas as pd

from lionel.constants import DATA
from lionel.db.connector import DBManager
from lionel.model.hierarchical import FPLPointsModel


def get_players_recent_games(dbm, season, gameweek, n_games):
    q = f"""
    SELECT * FROM prediction
    WHERE 
        (season = {season} AND gameweek > {gameweek - n_games} AND gameweek <= {gameweek})
        OR (season = {season - 1} AND gameweek > 38 - {n_games} + {gameweek} AND gameweek <= 38)
    """
    # Doesn't have value?
    df_recent = pd.read_sql(q, dbm.engine.raw_connection())
    df_recent["minutes"] = (
        df_recent.groupby("player")["minutes"].transform("mean").values
    )
    df_recent = (
        df_recent.sort_values(by=["season", "gameweek"]).groupby("player").tail(1)
    ).reset_index(drop=True)
    return df_recent  # [['player', 'position', 'minutes']]


def get_future_fixtures(dbm, season):
    q = f"""
    SELECT 
        fixtures.home_id, fixtures.away_id, 
        ht.name AS home_team, at.name AS away_team, 
        fixtures.gameweek, fixtures.season
    FROM fixtures 

    INNER JOIN teams as ht
    ON fixtures.home_id = ht.id

    INNER JOIN teams as at
    ON fixtures.away_id = at.id

    WHERE season = {season} AND home_score IS NULL
    """
    return pd.read_sql(q, dbm.engine.raw_connection())


def build_pred_data(dbm, gameweek, season, n_games=3):
    df_recent = get_players_recent_games(dbm, season, gameweek, n_games)
    df_fix = get_future_fixtures(dbm, season)

    # Merge home and away fixtures
    cols = ["player", "position", "minutes", "is_home", "team_id", "value"]
    df_h = df_fix.merge(
        df_recent[cols],
        left_on="home_id",
        right_on="team_id",
    ).drop(columns=["team_id", "home_id", "away_id"])
    df_a = df_fix.merge(
        df_recent[cols],
        left_on="away_id",
        right_on="team_id",
    ).drop(columns=["team_id", "home_id", "away_id"])
    df_h["is_home"] = 1
    df_a["is_home"] = 0
    df = pd.concat([df_h, df_a], axis=0).reset_index(drop=True)
    df[["home_goals", "away_goals"]] = None
    # assert df.shape[0] == df.player.nunique() * len(df.gameweek.unique()) # doesn't apply if teams have fewer fixtures
    return df


def predict_scoreline(df_pred, fplm):
    """
    Get scoreline predictions for a given dataframe and FPLM model.

    Args:
        df_pred (pandas.DataFrame): The input dataframe containing prediction data.
        fplm: The FPLM model used for prediction.

    Returns:
        pandas.DataFrame: A dataframe containing the scoreline predictions.

    Raises:
        AssertionError: If the index values of the dataframe do not match the expected values.

    """
    trace = fplm.predict_posterior(df_pred, predictions=True)

    df_ = trace[["home_goals", "away_goals"]].to_dataframe().reset_index(level="match")

    assert (df_.index.get_level_values("chain") == df_.chain).all()
    assert (df_.index.get_level_values("draw") == df_.draw).all()

    df_ = df_.reset_index(drop=True)

    home_teams = fplm.teams[fplm.model.home_team.eval()]
    away_teams = fplm.teams[fplm.model.away_team.eval()]
    match_id = fplm.model.coords["match"]
    df_2 = pd.DataFrame({"home": home_teams, "away": away_teams, "match": match_id})

    df = df_.merge(df_2, on="match", how="left")
    return df


def predict_players(df_pred, next_gw, fplm):
    """
    Get player predictions based on the given dataframe, next gameweek, and FPL model.

    Args:
        df_pred (pandas.DataFrame): The dataframe containing the player predictions.
        next_gw (int): The next gameweek.
        fplm (FPLModel): The FPL model used for predictions.

    Returns:
        pandas.DataFrame: The dataframe with player predictions, including mean and next points predictions.

    """
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
