import pandas as pd
import numpy as np

from lionel.model.hierarchical import FPLPointsModel


def build_future_fixture_df(dbm, season, next_gw):
    """
    Builds a DataFrame containing future fixtures relative to a given season and gameweek.

    Parameters:
    - dbm: The database manager object used to query the database.
    - season: The season for which fixtures are to be retrieved.
    - next_gw: The gameweek from which fixtures are to be retrieved.

    Returns:
    - df_fix: A DataFrame containing the season, gameweek, home team, and away team for future fixtures.
    """

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


def build_recent_games_df(df, season, next_gw, n_games=3):
    """
    Builds a dataframe containing recent games data for each player.

    Args:
        df (pandas.DataFrame): The input dataframe containing all games data.
        season (int): The current season.
        next_gw (int): The next gameweek.
        n_games (int, optional): The number of previous gameweeks to consider. Defaults to 3.

    Returns:
        pandas.DataFrame: The dataframe containing recent games data for each player.
    """

    # Filter games from the last 3 gameweeks
    df_recent = df[
        (
            (df["gameweek"] >= next_gw - n_games) & (df["season"] == season)
            | (df["gameweek"] >= 38 - (n_games - next_gw))
            & (df["season"] == season - 1)
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
    """
    Builds a prediction DataFrame based on the df of recent games and fixture df.

    Args:
        df_recent (pandas.DataFrame): The recent DataFrame containing recent team data.
        df_fix (pandas.DataFrame): The fix DataFrame containing fixture data.

    Returns:
        pandas.DataFrame: The prediction DataFrame with expected columns, points, and value.

    """
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


def get_player_predictions(df_pred, next_gw, fplm):
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


def get_scoreline_preds(df_pred, fplm):
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


def run(season, next_gw, dbm, fplm):
    """
    Run the prediction process for the given season and next game week.

    Args:
        season (str): The season for which the prediction is being run.
        next_gw (int): The next game week for which the prediction is being run.
        dbm (DatabaseManager): The database manager object.
        fplm (FPLModel): The FPL model object.

    Returns:
        pandas.DataFrame: The predicted data for the next 5 game weeks.
    """

    df_future_fixture = build_future_fixture_df(dbm, season, next_gw)
    df_recent = build_recent_games_df(fplm.X, season, next_gw)
    df_pred = build_pred_df(df_recent, df_future_fixture)
    df_pred_next_5 = get_player_predictions(df_pred, next_gw, fplm)

    # send these both to the database
    df_scoreline = get_scoreline_preds(df_pred, fplm)
    df_next_gw = (
        df_pred[df_pred.gameweek == next_gw][["home_team", "away_team"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    df_scoreline.to_sql("scorelines", dbm.engine, if_exists="replace", index=False)
    df_next_gw.to_sql("next_games", dbm.engine, if_exists="replace", index=False)
    return df_pred_next_5


if __name__ == "__main__":
    import sys
    from lionel.db.connector import DBManager
    from lionel.model.hierarchical import FPLPointsModel
    from lionel.constants import DATA

    dbm = DBManager(DATA / "fpl.db")
    fplm = FPLPointsModel.load(DATA / "analysis/hm_02.nc")
    season, next_gw = [int(x) for x in sys.argv[1:]]
    run(season, next_gw, dbm, fplm)
