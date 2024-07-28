import pandas as pd
import numpy as np
import datetime as dt
import lionel.data_load.storage.storage_handler as storage_handler


SEASON = 24
GAMES_WINDOW = 30
HISTORICAL_COLS = [
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

# sh = storage_handler.StorageHandler(local=True)


def load_gw_data(storage_handler, next_gw, season):
    df_gw_current = storage_handler.load(f"processed/gw_stats_{season}.csv")
    df_gw_current = df_gw_current[df_gw_current["gameweek"] < next_gw]
    df_gw_previous = storage_handler.load(f"processed/gw_stats_{season-1}.csv")
    assert (df_gw_current.columns == df_gw_previous.columns).all()
    df_gw = pd.concat([df_gw_previous, df_gw_current])
    return df_gw


def filter_unneeded_names(df_gw, season):
    # Only keep those available at the end of the year
    latest_gw = df_gw.loc[df_gw["season"] == season, "gameweek"].max()
    valid_names = (
        df_gw.loc[
            (df_gw["gameweek"] == latest_gw) & (df_gw["season"] == season), "name"
        ]
        .unique()
        .tolist()
    )
    df_gw = df_gw[df_gw["name"].isin(valid_names)]
    return df_gw


sh = storage_handler.StorageHandler(local=True)
df_load = load_gw_data(sh, 9, 24)


def get_gw_order(df_gw):
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
    return gameweek_order


def filter_gws(df_gw, next_gw, season, games_window=30):
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
    # to change this bit
    gameweek_order = get_gw_order(df_gw)
    df_gw = df_gw.merge(gameweek_order, on=["gameweek", "season"])
    return df_gw


def get_gw_order_map(df_f, SEASON):

    gw_map = df_f[["gameweek", "season", "gameweek_order"]].drop_duplicates()
    gw_map = df_f.loc[
        df_f.season == SEASON, ["gameweek", "gameweek_order"]
    ].drop_duplicates()
    gw_map = gw_map.set_index("gameweek").to_dict(orient="dict")["gameweek_order"]
    for gw in range(max(gw_map.keys()) + 1, 39):
        gw_map[gw] = gw_map[gw - 1] + 1
    return gw_map


def get_date_from_gw(gw):
    day0 = dt.date(2024, 1, 1)
    return day0 + dt.timedelta(days=int(gw))


def prepare_training_data(df):

    # Add scored + conceded
    df["team_scored"] = np.where(df["is_home"], df["team_h_score"], df["team_a_score"])
    df["team_conceded"] = np.where(
        df["is_home"], df["team_a_score"], df["team_h_score"]
    )
    df["y"] = df["total_points"]
    df["unique_id"] = df["name"]
    df["ds"] = pd.to_datetime(
        df["gameweek_order"].apply(get_date_from_gw)
    )  # apply after?

    df = df.drop(
        columns=["name", "game_date", "total_points", "gameweek", "gameweek_order"]
        + ["season", "element", "ict_index", "team_h_score"]
        + ["team_a_score", "opponent_team"]
    )
    df = pd.get_dummies(
        df, columns=["opponent_team_name", "team_name", "position"], drop_first=True
    )
    return df  ## this should be prepared fully now...


def fixture_gw_to_ds(next_gw_order, horizon=1):
    print(next_gw_order)
    next_ds = get_date_from_gw(next_gw_order)
    return {next_gw_order + i: next_ds + dt.timedelta(days=i) for i in range(horizon)}


def get_relevant_fixtures(next_gw, season, storage_handler, gw_map, horizon=1):

    # this might cause an issue if there's no game for a team in a
    # particular week... could just remove those preds in post though

    df_fixtures = storage_handler.load(f"processed/fixtures_{season}.csv")
    desired_gameweeks = list(range(next_gw, next_gw + horizon))
    df_fixtures = df_fixtures[df_fixtures["gameweek"].isin(desired_gameweeks)]
    df_fixtures["gameweek_order"] = df_fixtures["gameweek"].map(gw_map)
    df_fixtures["ds"] = df_fixtures["gameweek_order"].apply(
        lambda x: get_date_from_gw(x)
    )

    df_fixtures = pd.get_dummies(
        df_fixtures, columns=["team_name", "opponent_team_name"], drop_first=True
    )  # need to keep team name for merge?

    df_fixtures = df_fixtures.drop(columns=["gameweek", "game_date", "gameweek_order"])
    df_fixtures["ds"] = pd.to_datetime(df_fixtures["ds"])
    df_fixtures["merge_team"] = _merge_team_col(df_fixtures)
    return df_fixtures


def _merge_team_col(df):
    # this doesn't work because one dummy column is dropped
    df2 = df.copy()
    df2["team_name_Arsenal"] = np.where(
        df2[
            [col for col in df.columns if "team_name" in col and "opponent_" not in col]
        ].any(axis=1),
        False,
        True,
    )
    return (
        df2[
            [
                col
                for col in df2.columns
                if "team_name" in col and "opponent_" not in col
            ]
        ]
        .idxmax(axis=1)
        .str.split("_")
        .str[-1]
    )


def create_left_of_merge(df_tr, horizon):
    df_te = df_tr.sort_values("ds").groupby("unique_id").tail(1).reset_index(drop=True)
    first_gw_in_forecast = df_te["ds"].max() + dt.timedelta(days=1)

    df_te["ds"] = first_gw_in_forecast
    for h in range(1, horizon):
        gw_df = df_te.copy()
        gw_df["ds"] = gw_df["ds"] + dt.timedelta(days=h)
        df_te = pd.concat([df_te, gw_df])

    df_te["merge_team"] = _merge_team_col(df_te)
    df_te = df_te[
        ["unique_id", "ds", "merge_team"]
        + [col for col in df_te.columns if "position_" in col]
    ]
    return df_te


def create_test_train(next_gw, season=24, games_window=30, horizon=1):
    df = load_gw_data(sh, next_gw, season)
    df = filter_unneeded_names(df, next_gw)
    df_f = filter_gws(df, next_gw, season, games_window)
    gw_map = get_gw_order_map(df_f, season)
    df_tr = prepare_training_data(df_f)
    df_fixtures = get_relevant_fixtures(next_gw, season, sh, gw_map, horizon)
    df_te = create_left_of_merge(df_tr, horizon)
    df_te = df_te.merge(df_fixtures, on=["merge_team", "ds"], how="left")
    df_te = df_te.fillna(False)
    df_te = df_te.drop(columns=["merge_team"])

    hist_exog_list = [
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
        # "y",
    ]
    futr_exog_list = [
        col for col in df_te.columns if "opponent_team_" in col or "position_" in col
    ] + ["is_home"]

    return {
        "train": df_tr,
        "test": df_te,
        "futr_exog_list": futr_exog_list,
        "hist_exog_list": hist_exog_list,
    }


# df_tr, df_te = create_test_train(next_gw=9)
# df_te

# SEASON = 24
# GAMES_WINDOW = 30
# NEXT_GW = 2
# HORIZON = 1
# df = load_gw_data(sh, SEASON)
# df_f = filter_gws(df, NEXT_GW, SEASON, GAMES_WINDOW)
# gw_map = get_gw_order_map(df_f, SEASON)
# df_tr = prepare_training_data(df_f)

# # merge team col is appearing twice here...
# df_fixtures = get_relevant_fixtures(NEXT_GW, SEASON, sh, gw_map, HORIZON)

# df_te = create_left_of_merge(df_tr, HORIZON) # this doubled size...
# # output should stay this size?


# # need to split and merge home on fixtures and vice versa
# df_te = df_te.merge(df_fixtures, on=["merge_team", "ds"], how="left") ## added some rows - why?

# # gotta fill missing vals
# df_te = df_te.fillna(False)
# df_te = df_te.drop(columns=["merge_team"])
# df_te.to_csv("TEST3.csv")

# # for fixtures, get those within the horizon from current gameweek.
# # map the gameweek order to them from origin

# # for players, get the last observation for each player
# # and just add an observation for each player for each gameweek in the horizon

# #

# # this gives me the correct number of rows for forecast. Now need to add opponent, home away etc...

# # won't account properly for double GWs! maybe something to do - try and get two forecasts out


# # df_fixtures.ds.unique()

# df_g = df_te.merge(df_fixtures, on=["merge_team", "ds"], how="left")
# # each player is duplicated home and away?
# # df_te
# df_g.to_csv("TEST.csv", index=False)


# df_g = df_te[['merge_team', 'unique_id']].drop_duplicates()
# df_fixtures.join(df_g.set_index('merge_team'), how='cross') # fixtures has duplicates - don't want to do that...

# df_te.shape[0] * df_fixtures.shape[0]

# df_h = df_fixtures.join(df_g.set_index('merge_team'), how='cross')


# df_gw_current = sh.load(f"processed/gw_stats_{24}.csv")
# latest_gw = df_gw_current["gameweek"].max()
# valid_names = (
#     df_gw_current.loc[df_gw_current["gameweek"] == latest_gw, "name"].unique().tolist()
# )

# only keep the names that are still in the training set at the end... just makes it easier
