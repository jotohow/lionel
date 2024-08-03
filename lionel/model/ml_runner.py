from mlforecast import MLForecast
from mlforecast.lag_transforms import (
    ExpandingMean,
    RollingMean,
    ExponentiallyWeightedMean,
    ExpandingStd,
)
import lightgbm as lgb
from lionel.model.naive import Naive


def forecast(
    df_train,
    df_test,
    horizon=1,
    futr_exog_list=[],
    models=[Naive(), lgb.LGBMRegressor()],
):
    fcst = MLForecast(
        models=models,
        freq=1,
        lags=[1],
        lag_transforms={
            1: [
                ExpandingStd(),
                ExponentiallyWeightedMean(alpha=0.1),
                ExponentiallyWeightedMean(alpha=0.5),
            ]
        },
    )
    fcst.fit(df_train, static_features=[] if futr_exog_list else None)
    preds = fcst.predict(h=horizon, X_df=df_test if futr_exog_list else None)
    return preds


def run(df, horizon=1):
    future_exog_list = [
        col
        for col in df.columns
        if col.startswith("opponent_team_name")
        or col.startswith("team_name")
        or col.startswith("position")
    ]
    train_indices = df[df.game_complete].index
    df_train = df.loc[train_indices]
    df_test = df.loc[~df.index.isin(train_indices)]

    # Run with no exogenous features
    df_train_1 = df_train[["ds", "unique_id", "y"]]
    df_test_1 = df_test[["ds", "unique_id", "y"]]
    preds_1 = forecast(df_train_1, df_test_1, horizon=horizon)
    preds_1 = preds_1.rename(columns={"LGBMRegressor": "LGBMRegressor_no_exog"})

    # Run with exogenous features
    df_train_2 = df_train[["ds", "unique_id", "y"] + future_exog_list]
    df_test_2 = df_test[["ds", "unique_id", "y"] + future_exog_list]
    preds_2 = forecast(
        df_train_2,
        df_test_2,
        horizon=horizon,
        futr_exog_list=future_exog_list,
        models=[lgb.LGBMRegressor()],
    )
    preds_2 = preds_2.rename(columns={"LGBMRegressor": "LGBMRegressor_with_exog"})

    preds = preds_1.merge(preds_2, on=["ds", "unique_id"], how="inner")
    return preds
