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
