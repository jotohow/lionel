import pandas as pd
import numpy as np
import datetime as dt
import lionel.data_load.storage.storage_handler as storage_handler


def load_gw_data(storage_handler, next_gw, season):
    df_gw_current = storage_handler.load(f"processed/gw_stats_{season}.csv")
    df_gw_current = df_gw_current[df_gw_current["gameweek"] < next_gw]
    df_gw_previous = storage_handler.load(f"processed/gw_stats_{season-1}.csv")
    assert (df_gw_current.columns == df_gw_previous.columns).all()
    df_gw = pd.concat([df_gw_previous, df_gw_current])
    return df_gw


def get_date_from_gw(gw):
    day0 = dt.date(2024, 1, 1)
    return day0 + dt.timedelta(days=int(gw))


def get_expected_names3(df_gw, season, next_gw):
    # return a series of names against their team in the last gameweek
    exp = df_gw.loc[
        (df_gw["season"] == season) & (df_gw["gameweek"] == next_gw - 1),
        ["name", "team_name", "position"],
    ].set_index("name")
    return exp.to_dict(orient="index")


def filter_unneeded_names(df_gw, expected_names):
    return df_gw[df_gw["name"].isin(expected_names)]


def create_forecast_df(expected_names, next_gw, horizon):
    l_forecast = []

    for gw in range(next_gw, next_gw + horizon):
        # create one dict for each player in each gw
        d_ = [
            {
                "name": name,
                "gameweek": gw,
                "team_name": thing["team_name"],
                "position": thing["position"],
            }
            for name, thing in expected_names.items()
        ]
        l_forecast.extend(d_)
    return pd.DataFrame.from_dict(l_forecast)


def add_fixtures_to_forecast(df_forecast, df_fixtures):
    df_forecast = df_forecast.merge(
        df_fixtures, on=["gameweek", "team_name"], how="left", indicator=True
    )
    assert df_forecast[df_forecast._merge == "both"].notna().all().all()
    df_forecast = df_forecast.ffill()

    df_no_fixture = df_forecast.loc[
        df_forecast["_merge"] == "left_only", ["name", "gameweek", "team_name"]
    ]
    df_forecast = df_forecast.drop(columns="_merge")

    # For now, only predict the first game of a gameweek if it's a double...
    df_forecast = df_forecast.drop_duplicates(
        subset=["name", "gameweek"], keep="first"
    ).reset_index(drop=True)
    return df_forecast, df_no_fixture


def add_dummy_cols(df_filtered, df_forecast):
    # TODO: args were df_train, df_forecast before... not sure if i messed smth up
    df_train = pd.get_dummies(
        df_filtered,  # THIS
        columns=["team_name", "position", "opponent_team_name"],
        drop_first=True,
    )
    expected_dummy_cols = [
        col
        for col in df_train.columns
        if "team_name" in col or "opponent_team_name" in col or "position" in col
    ]
    df_forecast = pd.get_dummies(
        df_forecast,
        columns=["team_name", "position", "opponent_team_name"],
        drop_first=True,
    )
    missing_dummy_cols = [
        col for col in expected_dummy_cols if col not in df_forecast.columns
    ]
    extra_dummy_cols = pd.DataFrame(
        {col: [False] * len(df_forecast) for col in missing_dummy_cols}
    )
    df_forecast = pd.concat([df_forecast, extra_dummy_cols], axis=1)
    return df_train, df_forecast


def get_gw_map(df_gw, season):
    gameweek_order = (
        df_gw[["gameweek", "season"]]
        .drop_duplicates()
        .sort_values(["season", "gameweek"], ascending=True)
    )
    gameweek_order = (
        gameweek_order.reset_index(drop=True)
        .reset_index(drop=False)
        .rename(columns={"index": "gameweek_order"})
    )
    gameweek_order["gameweek_order"] = gameweek_order["gameweek_order"] + 1

    # Add missing ones up to end of season
    last_gw = gameweek_order.tail(1)
    last_order = last_gw["gameweek_order"].values[0]
    last_gw = last_gw["gameweek"].values[0]

    diff = 39 - last_gw
    l = []
    for i in range(1, diff):
        d = {
            "gameweek_order": last_order + i,
            "gameweek": last_gw + i,
            "season": season,
        }
        l.append(d)
    df = pd.DataFrame.from_dict(l)
    gameweek_order = pd.concat([gameweek_order, df])
    gameweek_order = gameweek_order.set_index(["season", "gameweek"]).to_dict(
        orient="index"
    )
    return gameweek_order


def filter_gws(df_gw, next_gw, season, games_window=20):
    df_gw = df_gw[
        (  # current season
            (df_gw["gameweek"] < next_gw)
            & (df_gw["gameweek"] > next_gw - games_window)
            & (df_gw["season"] == season)
        )
        | (  # last season within games_window
            (df_gw["gameweek"] > next_gw + 38 - games_window)
            & (df_gw["season"] == season - 1)
        )
    ]
    return df_gw


def add_gameweek_order(df_train, df_forecast, season):
    gw_map = get_gw_map(df_train, season)
    df_forecast["season"] = season
    df_l = [df_train, df_forecast]
    for i in range(len(df_l)):
        df_l[i]["gameweek_order"] = (
            df_l[i]
            .set_index(["season", "gameweek"])
            .index.map(lambda x: gw_map[x]["gameweek_order"])
        )
        df_l[i]["ds"] = pd.to_datetime(
            df_l[i]["gameweek_order"].apply(get_date_from_gw)
        )
    return df_l


def filter_relevant_cols(df_train, df_forecast):
    # TODO: move this elsewhere as it's not working well like this
    dummy_cols = [
        col
        for col in df_train.columns
        if "team_name" in col or "opponent_team_name" in col or "position" in col
    ] + ["is_home"]
    id_cols = ["unique_id", "ds"]
    keep_cols_tr = (
        [
            "assists",
            "bps",
            "creativity",
            "goals_scored",
            "influence",
            "minutes",
            "selected",
            "threat",
            "transfers_balance",
            "value",
            "team_scored",
            "team_conceded",
            "y",
        ]
        + dummy_cols
        + id_cols
    )
    df_train = df_train[keep_cols_tr]
    df_forecast["unique_id"] = df_forecast["name"]
    df_forecast = df_forecast[dummy_cols + id_cols]
    return df_train, df_forecast, dummy_cols, id_cols


def run(next_gw, season=24, horizon=1, games_window=20):
    # also need this to export futr_exog_list, hist_exog_list
    # Load datasets
    sh = storage_handler.StorageHandler(local=True)
    df_load = load_gw_data(sh, next_gw, season)
    df_fixtures = sh.load(f"processed/fixtures_{season}.csv")

    # Get expected names and filter
    expected_names = get_expected_names3(df_load, season, next_gw)
    df_filtered = filter_unneeded_names(df_load, expected_names.keys())

    # Create the forecast df
    df_forecast = create_forecast_df(expected_names, next_gw, horizon)
    df_forecast, df_no_fixture = add_fixtures_to_forecast(df_forecast, df_fixtures)

    # Add the dummy cols and filter
    df_train, df_forecast = add_dummy_cols(df_filtered, df_forecast)
    df_train = filter_gws(df_train, next_gw, season, games_window).reset_index(
        drop=True
    )

    df_train, df_forecast = add_gameweek_order(df_train, df_forecast, season)

    # assert that there are no missing names in test
    df_train_names = list(df_train["name"].unique())
    df_forecast_names = list(df_forecast["name"].unique())
    assert set(df_forecast_names) == (set(df_train_names))

    df_train["team_scored"] = np.where(
        df_train["is_home"], df_train["team_h_score"], df_train["team_a_score"]
    )
    df_train["team_conceded"] = np.where(
        df_train["is_home"], df_train["team_a_score"], df_train["team_h_score"]
    )
    df_train["y"] = df_train["total_points"]
    df_train["unique_id"] = df_train["name"]

    df_train, df_forecast, dummy_cols, id_cols = filter_relevant_cols(
        df_train, df_forecast
    )
    return (df_train, df_forecast, dummy_cols, id_cols)


if __name__ == "__main__":
    season = 24
    next_gw = 25
    horizon = 1
    df_train, df_forecast = run(next_gw, season, horizon)

    print(df_train.head())
    print(df_forecast.head())
