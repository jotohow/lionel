from neuralforecast import NeuralForecast
from lionel.model.lstm_with_relu import LSTMWithReLU


# temp solutions
ID_COLS = ["unique_id", "ds"]
DUMMIES = [
    "team_name_Aston Villa",
    "team_name_Bournemouth",
    "team_name_Brentford",
    "team_name_Brighton",
    "team_name_Burnley",
    "team_name_Chelsea",
    "team_name_Crystal Palace",
    "team_name_Everton",
    "team_name_Fulham",
    "team_name_Leeds",
    "team_name_Leicester",
    "team_name_Liverpool",
    "team_name_Luton",
    "team_name_Manchester City",
    "team_name_Manchester Utd",
    "team_name_Newcastle",
    "team_name_Nottingham",
    "team_name_Sheffield Utd",
    "team_name_Southampton",
    "team_name_Tottenham",
    "team_name_West Ham",
    "team_name_Wolves",
    "position_FWD",
    "position_GK",
    "position_MID",
    "opponent_team_name_Aston Villa",
    "opponent_team_name_Bournemouth",
    "opponent_team_name_Brentford",
    "opponent_team_name_Brighton",
    "opponent_team_name_Burnley",
    "opponent_team_name_Chelsea",
    "opponent_team_name_Crystal Palace",
    "opponent_team_name_Everton",
    "opponent_team_name_Fulham",
    "opponent_team_name_Leeds",
    "opponent_team_name_Leicester",
    "opponent_team_name_Liverpool",
    "opponent_team_name_Luton",
    "opponent_team_name_Manchester City",
    "opponent_team_name_Manchester Utd",
    "opponent_team_name_Newcastle",
    "opponent_team_name_Nottingham",
    "opponent_team_name_Sheffield Utd",
    "opponent_team_name_Southampton",
    "opponent_team_name_Tottenham",
    "opponent_team_name_West Ham",
    "opponent_team_name_Wolves",
    "is_home",
]


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


def train_lstm(df, stat_exog_list, hist_exog_list, futr_exog_list):
    static_df = df[["unique_id"] + stat_exog_list].drop_duplicates(subset=["unique_id"])
    stat_exog_list = static_df.columns[1:].tolist()
    nf = get_lstm(hist_exog_list, futr_exog_list, stat_exog_list)
    nf.fit(df, static_df)
    return nf
