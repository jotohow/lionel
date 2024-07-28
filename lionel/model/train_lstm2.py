import pandas as pd
import numpy as np
import lionel.data_load.storage.storage_handler as storage_handler
from lionel.data_load.constants import CLEANED, ANALYSIS
import datetime as dt

from neuralforecast import NeuralForecast
from lionel.model.lstm_with_relu import LSTMWithReLU

STATIC_COLS = ["team_name", "position"]


def get_da_data():
    sh = storage_handler.StorageHandler(local=True)
    df = sh.load(CLEANED / "gw_stats_24.csv")
    return df


def get_date_from_gw(gw):
    day0 = dt.date(2024, 1, 1)
    return day0 + dt.timedelta(days=gw)


def prepare_da_data(df, static_cols=STATIC_COLS):

    df["team_scored"] = np.where(df["was_home"], df["team_h_score"], df["team_a_score"])
    df["team_conceded"] = np.where(
        df["was_home"], df["team_a_score"], df["team_h_score"]
    )
    df["y"] = df["total_points"]
    df["unique_id"] = df["name"]
    df["ds"] = pd.to_datetime(df["gameweek"].apply(get_date_from_gw))

    static_df = df[["unique_id"] + static_cols].drop_duplicates(subset=["unique_id"])
    df = df.drop(
        columns=static_cols
        + ["name", "game_date", "total_points", "gameweek"]
        + ["season", "element", "ict_index", "team_h_score", "team_a_score"]
    )

    df = pd.get_dummies(df, columns=["opponent_team"], drop_first=True)
    static_df = pd.get_dummies(
        static_df, columns=["team_name", "position"], drop_first=True
    )
    return df, static_df


def get_lstm(hist_exog_list, futr_exog_list, static_cols, horizon=1):
    models = [
        LSTMWithReLU(
            h=horizon,
            hist_exog_list=hist_exog_list,
            futr_exog_list=futr_exog_list,
            stat_exog_list=static_cols,
            scaler_type="robust",
            accelerator="cpu",
        )
    ]
    nf = NeuralForecast(models=models, freq="D")
    return nf


def train_lstm(df, static_df, hist_exog_list, futr_exog_list):
    static_cols = static_df.columns[1:].tolist()
    nf = get_lstm(hist_exog_list, futr_exog_list, static_cols)
    nf.fit(df, static_df)
    return nf


# static_cols = ['team_name', 'position']
futr_exog_list = (
    ["was_home"]
    + ["opponent_team_" + str(i) for i in range(2, 21)]
    + ["team_name", "position"]
)  # need to
hist_exog_list = [
    "value",
    "assists",
    "bps",
    "creativity",
    "influence",
    "minutes",
    "selected",
    "threat",
    "transfers_balance",
    "value",
    "team_scored",
    "team_conceded",
]

df = get_da_data()
df, static_df = prepare_da_data(df)

last_indices = df.sort_values("ds").groupby("unique_id").tail(1).index
tr = df.loc[~df.index.isin(last_indices)]
te = df.loc[df.index.isin(last_indices)]

nf = train_lstm(tr, static_df, hist_exog_list, futr_exog_list)
Y_hat = nf.predict(futr_df=te)

Y_hat.to_csv(ANALYSIS / "predictions.csv", index=True)
