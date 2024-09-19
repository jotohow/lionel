import pandas as pd
import numpy as np
from lionel.utils import undo_dummies

# TODO: Need to add the latest prices, positions maybe...


def prepare_data(df_train, df_preds, season, next_gw, alpha=0.5):

    df_train = df_train[~((df_train.season == season) & (df_train.gameweek >= next_gw))]
    for prefix, default in [
        ("team_name", "Arsenal"),
        ("position", "DEF"),
        ("opponent_team_name", "Arsenal"),
    ]:
        df_train = undo_dummies(df_train, prefix, default)

    df_train = df_train[
        ["unique_id", "season", "gameweek", "team_name", "position", "value"]
    ]
    df_train = (
        df_train.sort_values(by=["season", "gameweek"])
        .groupby("unique_id")
        .last()
        .reset_index()
    )

    df_predicted = (
        df_preds.sort_values(by="gameweek", ascending=False)
        .groupby("unique_id")
        .ewm(alpha=alpha)
        .mean()
        .reset_index()
        .sort_values(by="level_1")
        .drop(columns=["level_1", "gameweek"])
        .groupby("unique_id")
        .first()
        .reset_index()
    )
    df_predicted = df_predicted.rename(
        columns={colname: f"pred_{colname}" for colname in df_predicted.columns[1:]}
    )

    df_1 = df_train.merge(df_predicted, on="unique_id", how="inner")
    df_1 = df_1.drop(columns=["gameweek", "season"])
    return df_1
