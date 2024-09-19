import pandas as pd
import numpy as np
from lionel.utils import undo_dummies


def prepare_data_for_charts(df_train, df_preds):
    "Loads a player-gameweek level dataframe with predictions for charts."

    for prefix, default in [
        ("team_name", "Arsenal"),
        ("position", "DEF"),
        ("opponent_team_name", "Arsenal"),
    ]:
        df_train = undo_dummies(df_train, prefix, default)

    df_train = df_train[
        [
            "unique_id",
            "opponent_team_name",
            "is_home",
            "season",
            "gameweek",
            "team_name",
            "position",
            "y",
        ]
    ]
    df_preds = df_preds.rename(
        columns={
            "Naive": "y_Naive",
            "LGBMRegressor_no_exog": "y_LGBMRegressor_no_exog",
            "LGBMRegressor_with_exog": "y_LGBMRegressor_with_exog",
            "LSTMWithReLU": "y_LSTMWithReLU",
        }
    )
    df_full = df_train.merge(
        df_preds, on=["unique_id", "season", "gameweek"], how="left"
    )
    assert df_full.shape[0] == df_train.shape[0]
    maxes = df_full[df_full.y_Naive.notna()][["season", "gameweek"]].max()
    df_full = df_full[
        ~((df_full.season == maxes.season) & (df_full.gameweek > maxes.gameweek))
    ]
    df_full.loc[df_full.y_Naive.notna(), "y"] = np.nan
    df_full = df_full.rename(columns={"unique_id": "name"})
    return df_full


def build_selection_data(df_pred, next_gw, fplm):
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
    return df_pred


def get_scoreline_preds(df_pred, fplm):
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
