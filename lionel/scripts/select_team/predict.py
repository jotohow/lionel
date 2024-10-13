import numpy as np
import pandas as pd

from lionel.constants import DATA
from lionel.db.connector import DBManager
from lionel.model.hierarchical import FPLPointsModel


def get_players_recent_games(dbm, season, gameweek, n_games):
    q = f"""
    SELECT 
        CONCAT(p.id, "_", p.name) AS player,
        ps.position,
        CASE WHEN s.is_home = 1 THEN f.home_id ELSE f.away_id END AS team_id,
        f.gameweek, f.season,
        s.minutes, s.total_points, s.goals_scored, s.assists, s.is_home

    FROM stats AS s
    INNER JOIN fixtures AS f
    ON s.fixture_id = f.id

    INNER JOIN player_seasons as ps
    ON s.player_id = ps.id

    INNER JOIN players as p
    ON ps.player_id = p.id

    WHERE 
        (f.season = {season} AND f.gameweek > {gameweek - n_games} AND f.gameweek <= {gameweek})
        OR (f.season = {season - 1} AND f.gameweek > 38 - {n_games} + {gameweek} AND f.gameweek <= 38)

    """

    df_recent = pd.read_sql(q, dbm.engine.raw_connection())
    df_recent["minutes"] = (
        df_recent.groupby("player")["minutes"].transform("mean").values
    )
    df_recent = (
        df_recent.sort_values(by=["season", "gameweek"]).groupby("player").tail(1)
    )
    return df_recent


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
    cols = ["player", "position", "minutes", "is_home", "team_id"]

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
    assert df.shape[0] == df.player.nunique() * len(df.gameweek.unique())
    return df
