import pandas as pd
import lionel.data_load.storage.storage_handler as storage_handler
import lionel.data_load.process.process_train_data as load_data
import numpy as np

# TODO: Need to add the latest prices, positions maybe...


def _undo_dummies(df, prefix, default):
    cols = [col for col in df.columns if col.startswith(prefix)]
    df[prefix] = pd.from_dummies(df[cols], default_category=default)
    df[prefix] = df[prefix].str.split("_").str[-1]
    df = df.drop(columns=cols)
    return df


# def prepare_data(storage_handler, season, next_gw, alpha=0.5):
def prepare_data(df_train, df_preds, season, next_gw, alpha=0.5):

    df_train = df_train[~((df_train.season == season) & (df_train.gameweek >= next_gw))]

    team_cols = [col for col in df_train.columns if col.startswith("team_name_")]
    position_cols = [col for col in df_train.columns if col.startswith("position_")]
    df_train["team_name"] = pd.from_dummies(
        df_train[team_cols], default_category="Arsenal"
    )
    df_train["position"] = pd.from_dummies(
        df_train[position_cols], default_category="DEF"
    )

    df_train = df_train.drop(team_cols + position_cols, axis=1)
    for col in ["team_name", "position"]:
        df_train[col] = df_train[col].str.split("_").str[-1]

    df_train = df_train[
        ["unique_id", "season", "gameweek", "team_name", "position", "value"]
    ]
    df_train = (
        df_train.sort_values(by=["season", "gameweek"])
        .groupby("unique_id")
        .last()
        .reset_index()
    )

    # TODO: Update this to use exponential weighted mean
    # df_predicted = (
    #     df_preds.groupby("unique_id").mean().reset_index().drop(columns="gameweek")
    # )
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


def prepare_data_for_charts(df_train, df_preds):
    for prefix, default in [
        ("team_name", "Arsenal"),
        ("position", "DEF"),
        ("opponent_team_name", "Arsenal"),
    ]:
        df_train = _undo_dummies(df_train, prefix, default)

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
