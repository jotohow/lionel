import pandas as pd
import numpy as np
import datetime as dt
import itertools
import lionel.data_load.storage.storage_handler as storage_handler


def load_gw_data(storage_handler, next_gw, season):
    df_gw_current = storage_handler.load(f"processed/gw_stats_{season}.csv")
    df_gw_current = df_gw_current[df_gw_current["gameweek"] < next_gw]
    df_gw_previous = storage_handler.load(f"processed/gw_stats_{season-1}.csv")
    assert (df_gw_current.columns == df_gw_previous.columns).all()
    df_gw = pd.concat([df_gw_previous, df_gw_current])
    COLS = [
        "name",
        "gameweek",
        "season",
        "game_date",
        "team_name",
        "assists",
        "bps",
        "creativity",
        "goals_scored",
        "ict_index",
        "influence",
        "minutes",
        "position",
        "selected",
        "team_a_score",
        "team_h_score",
        "threat",
        "total_points",
        "transfers_balance",
        "value",
    ]
    df_gw["game_date"] = pd.to_datetime(pd.to_datetime(df_gw["game_date"]).dt.date)
    return df_gw[COLS]


def load_fixtures(storage_handler, season):
    df_fixtures_current = storage_handler.load(f"processed/fixtures_{season}.csv")
    df_fixtures_current["season"] = season
    df_fixtures_previous = storage_handler.load(f"processed/fixtures_{season-1}.csv")
    df_fixtures_previous["season"] = season - 1
    assert (df_fixtures_current.columns == df_fixtures_previous.columns).all()
    return pd.concat([df_fixtures_previous, df_fixtures_current])


def split_fixtures(next_gw, season, f):
    f_future = f[(f["season"] >= season) & (f["gameweek"] >= next_gw)]
    f_past = f[
        (f["season"] < season) | ((f["season"] == season) & (f["gameweek"] < next_gw))
    ]
    return f_past, f_future


def order_past_games(f_past):
    f_past = f_past.sort_values(
        ["team_name", "season", "gameweek", "game_date"],
        ascending=[True, False, False, False],
    )
    f_past["gw_i"] = f_past.groupby(["team_name"]).cumcount()
    max_gw_i = f_past["gw_i"].max()
    f_past["ds"] = max_gw_i - f_past["gw_i"]
    f_past["game_complete"] = True
    return f_past, max_gw_i


def order_future_games(f_future, max_gw_i):
    f_future = f_future.sort_values(
        ["team_name", "season", "gameweek", "game_date"],
        ascending=[True, True, True, True],
    )
    f_future["gw_number"] = f_future.groupby(["team_name", "gameweek"]).cumcount() + 1
    a = (
        f_future.groupby(["team_name", "gameweek"])
        .size()
        .reset_index()
        .groupby("gameweek")[0]
        .max()
    )
    gw_order = list(
        itertools.chain.from_iterable([[k] * v for k, v in a.to_dict().items()])
    )
    expected_gws = pd.DataFrame(gw_order, columns=["gameweek"])
    expected_gws["ds"] = expected_gws.index + max_gw_i + 1
    expected_gws["gw_number"] = expected_gws.groupby("gameweek").cumcount() + 1
    team_names = pd.DataFrame(
        sorted(list(f_future["team_name"].unique())), columns=["team_name"]
    )
    expected = team_names.merge(expected_gws, how="cross")

    f_future = expected.merge(
        f_future, how="left", on=["team_name", "gameweek", "gw_number"]
    )
    f_future["game_complete"] = False
    return f_future


def create_fixture_df(storage_handler, next_gw, season):
    fixtures = load_fixtures(storage_handler, season)
    f_past, f_future = split_fixtures(next_gw, season, fixtures)
    f_past, max_gw_i = order_past_games(f_past)
    f_future = order_future_games(f_future, max_gw_i)

    f_full = pd.concat([f_past, f_future])
    f_full = f_full.sort_values(["team_name", "ds"])
    f_full = f_full[
        [
            "team_name",
            "opponent_team_name",
            "is_home",
            "season",
            "gameweek",
            "game_date",
            "ds",
            "game_complete",
        ]
    ].reset_index(drop=True)
    f_full["game_date"] = pd.to_datetime(f_full["game_date"])
    return f_full


def get_expected_names3(df_gw, season, next_gw):
    # return a series of names against their team in the last gameweek
    if next_gw == 1:
        demoted_teams = ["Burnley", "Luton", "Sheffield Utd"]
        df_gw = df_gw[~df_gw["team_name"].isin(demoted_teams)]
        season = season - 1
        next_gw = 39
    exp = (
        df_gw.loc[
            (df_gw["season"] == season) & (df_gw["gameweek"] == next_gw - 1),
            ["name", "team_name", "position"],
        ]
        .drop_duplicates()
        .set_index("name")
    )
    print(len(exp))
    # exp.to_csv("TEST.csv")
    return exp.to_dict(orient="index")


# TODO: Aligning future fixtures like this might damage the preds
# because they're done recursively. Could be better to align just in order and then
# map games to gameweeks?
def run(storage_handler, next_gw, season):

    # Get fixtures list
    f = create_fixture_df(storage_handler, next_gw, season)

    # Get players data
    # needs to have position for next bit
    df_players = load_gw_data(storage_handler, next_gw, season)
    expected_names = get_expected_names3(df_players, season, next_gw)
    df_expected = (
        pd.DataFrame.from_dict(expected_names, orient="index")
        .reset_index()
        .rename(columns={"index": "name"})
    )

    # Merge everything together
    df_expected = (
        f.merge(df_expected, how="left", on="team_name")
        .sort_values(["team_name", "name", "ds"])
        .reset_index(drop=True)
        .drop(columns=["position"])  # dropped the gw arg here
    )
    df_final = df_expected.merge(
        df_players.drop(columns=["gameweek"]),  # added the gw arg here
        how="left",
        on=["team_name", "game_date", "season", "name"],
    )
    df_final = df_final[
        ((df_final["game_complete"]) & df_final.goals_scored.notna())
        | (~df_final["game_complete"])
    ]
    df_final["valid_game"] = (~df_final.game_complete) & (
        df_final.opponent_team_name.notna()
    ) | (df_final.game_complete)
    df_final = df_final.ffill()
    df_final = pd.get_dummies(
        df_final,
        columns=["team_name", "opponent_team_name", "position"],
        drop_first=True,
    )
    df_final = df_final.rename(columns={"total_points": "y", "name": "unique_id"})

    return df_final


if __name__ == "__main__":
    sh = storage_handler.StorageHandler(local=True)
    season = 24
    next_gw = 27
    # horizon = 1
    df = run(sh, next_gw, season)
    sh.store(df, f"analysis/train_{next_gw}_{season}.csv", index=False)
    # print(df_train.head())
    # print(df_forecast.head())


# import lionel.data_load.storage.storage_handler as storage_handler

# sh = storage_handler.StorageHandler(local=True)
# season = 24
# next_gw = 25
# gw_horizon = 5

# df = load_gw_data(sh, next_gw, season)
# expected_names = get_expected_names3(df, season, next_gw)
