import pandas as pd
import lionel.data_load.storage.storage_handler as storage_handler
import lionel.model.train_lstm as train_lstm


def run(storage_handler, next_gw, static=True, season=24, horizon=1, games_window=15):
    df_train = storage_handler.load(
        f"analysis/train_{games_window}_{next_gw}_{season}.csv"
    )
    df_test = storage_handler.load(
        f"analysis/test_{games_window}_{next_gw}_{season}.csv"
    )
    df_train["ds"] = pd.to_datetime(df_train["ds"])
    df_test["ds"] = pd.to_datetime(df_test["ds"])

    hist_exog_list = [
        col
        for col in df_train.columns
        if col not in train_lstm.ID_COLS and col not in train_lstm.DUMMIES
    ]
    stat_exog_list = [
        col
        for col in train_lstm.DUMMIES
        if col.startswith("team_name_") or col.startswith("position_")
    ]
    futr_exog_list = (
        [col for col in train_lstm.DUMMIES if col not in stat_exog_list]
        if static
        else train_lstm.DUMMIES
    )
    nf = train_lstm.train_lstm(
        df_train, stat_exog_list, hist_exog_list, futr_exog_list, horizon
    )

    preds = nf.predict(futr_df=df_test)
    storage_handler.store(
        preds, f"analysis/preds_{games_window}_{next_gw}_{season}.csv"
    )


if __name__ == "__main__":
    next_gw = 25
    horizon = 1
    sh = storage_handler.StorageHandler(local=True)
    run(sh, next_gw, horizon=horizon)
