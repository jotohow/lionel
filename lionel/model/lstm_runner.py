from neuralforecast import NeuralForecast
from lionel.model.lstm import LSTMWithReLU


def forecast(
    df_train,
    df_test,
    horizon,
    futr_exog_list=[],
    hist_exog_list=[],
    input_size=20,
    models=[LSTMWithReLU],
):
    models = [
        m(
            h=horizon,
            # input_size=20,
            futr_exog_list=futr_exog_list,
            hist_exog_list=hist_exog_list,
            # stat_exog_list = stat_exog_list if STATIC else None,
            scaler_type="robust",
            accelerator="cpu",
            max_steps=1000,
        )
        for m in models
    ]
    nf = NeuralForecast(models=models, freq=1)
    nf.fit(df=df_train)
    return nf.predict(futr_df=df_test).reset_index()


def run(df, horizon=1):
    futr_exog_list = [
        col
        for col in df.columns
        if col.startswith("position")
        # or col.startswith("opponent_team_name")
        # or col.startswith("team_name")
        or col in ["strength_attack", "strength_defence"]
    ]
    hist_exog_list = [
        "assists",
        "bps",
        "creativity",
        "goals_scored",
        "ict_index",
        "influence",
        "minutes",
        "selected",
        "team_a_score",
        "team_h_score",
        "threat",
        "transfers_balance",
        "value",
        "y",
    ]
    cols = [
        col
        for col in df.columns
        if col in futr_exog_list + hist_exog_list + ["y", "ds", "unique_id"]
    ]
    train_indices = df[df.game_complete].index
    df = df[cols]
    df_train = df.loc[train_indices]
    df_test = df.loc[~df.index.isin(train_indices)]
    preds = forecast(
        df_train,
        df_test,
        horizon=horizon,
        futr_exog_list=futr_exog_list,
        hist_exog_list=hist_exog_list,
    )
    return preds
