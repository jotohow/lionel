import pandas as pd
import lionel.data_load.storage.storage_handler as storage_handler
import lionel.data_load.process.process_train_data as load_data

# import lionel.team.select as select


def prepare_data(storage_handler, season, next_gw, alpha=0.5):
    df_preds = storage_handler.load(f"analysis/preds_{next_gw}_{season}.csv")
    df_train = storage_handler.load(f"analysis/train_{next_gw}_{season}.csv")
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


# NB: This code would achieve it the EWM that I want
# b = df_preds.sort_values(by='gameweek', ascending=False).groupby('unique_id').ewm(alpha=0.75).mean().reset_index().sort_values(by='level_1')
# b[b.unique_id=='Bukayo Saka']
