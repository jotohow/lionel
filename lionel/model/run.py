import lionel.model.lstm_runner as lstm_runner
import lionel.model.ml_runner as ml_runner
from lionel.model.prepare_data import prepare_data_for_charts


def get_horizon(valid_games, next_gw, season, gw_horizon):
    """
    Calculate horizon from game week horizon

    Given a horizon in terms of game weeks, convert it to a game index that
    accounts for missing and double game weeks.

    Args:
        valid_games (DataFrame): DataFrame containing valid games data.
        next_gw (int): The next game week.
        season (str): The season.
        gw_horizon (int): The horizon for the game week.

    Returns:
        int: The difference between the maximum and minimum ds values.

    Raises:
        Exception: If gw_horizon exceeds the length of the season.
    """
    if gw_horizon + next_gw > 38:
        raise Exception("gw_horizon exceeds season length")

    max_ds = valid_games[
        (valid_games["gameweek"] == next_gw + gw_horizon)
        & (valid_games["season"] == season)
    ].ds.max()
    min_ds = valid_games[
        (valid_games["gameweek"] == next_gw) & (valid_games["season"] == season)
    ].ds.unique()
    assert min_ds.shape[0] == 1
    min_ds = min_ds.min()
    return max_ds - min_ds


def run(sh, df, season, next_gw, gw_horizon=1):
    """
    Run the models and generate predictions for the given dataframe.

    Args:
        df (pandas.DataFrame): The input train dataframe containing the data for predictions.
        season (str): The season for which predictions are being made.
        next_gw (int): The next gameweek for which predictions are being made.
        gw_horizon (int, optional): The number of gameweeks to consider for predictions. Defaults to 1.

    Returns:
        pandas.DataFrame: The predictions dataframe containing the aggregated predictions for each unique_id and gameweek.
    """

    # Create a map of valid games and ds to gameweek
    valid_games = df[df["valid_game"]][["unique_id", "ds", "gameweek", "season"]]
    horizon = get_horizon(valid_games, next_gw, season, gw_horizon)

    # Run the models
    preds_lstm = lstm_runner.run(df, horizon)
    preds_ml = ml_runner.run(df, horizon)
    assert (
        preds_lstm.shape[0] == preds_ml.shape[0]
    ), "Predictions have different lengths"
    assert preds_lstm[["ds", "unique_id"]].equals(preds_ml[["ds", "unique_id"]])

    # Collect everything together and clean up
    preds = preds_ml.merge(preds_lstm, on=["ds", "unique_id"])
    preds = preds.merge(valid_games, on=["ds", "unique_id"], how="inner")
    preds = (
        preds.groupby(["unique_id", "gameweek"])
        .sum()
        .reset_index()
        .drop(columns=["ds", "season"])
    )
    preds["season"] = season
    sh.store(preds, f"analysis/preds_{next_gw}_{season}.csv", index=False)

    # Prepare data for charts
    df_charts = prepare_data_for_charts(df, preds)
    sh.store(df_charts, f"analysis/charts_{next_gw}_{season}.csv", index=False)
    return preds
