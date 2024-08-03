import lionel.model.lstm_runner as lstm_runner
import lionel.model.ml_runner as ml_runner


def get_horizon(valid_games, next_gw, season, gw_horizon):

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


def get_valid_games_horizon(df, gw_horizon):
    valid_games = df[df["valid_game"]][["unique_id", "ds", "gameweek", "season"]]
    next_gw = df[~df.game_complete].gameweek.min()
    season = df.season.max()
    horizon = get_horizon(valid_games, next_gw, season, gw_horizon)
    return valid_games, horizon


def run(df, season, next_gw, gw_horizon=1):

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

    return preds
