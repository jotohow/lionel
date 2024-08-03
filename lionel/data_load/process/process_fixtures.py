import pandas as pd


def add_team_names(df_fixtures, df_team_ids):

    # Add team IDs
    df_fixtures = df_fixtures.merge(
        df_team_ids, left_on="team_a", right_on="team_id", how="left"
    )
    df_fixtures = df_fixtures.merge(
        df_team_ids, left_on="team_h", right_on="team_id", how="left"
    )

    # Cleaning
    df_fixtures = df_fixtures.rename(
        {"team_name_x": "away_team_name", "team_name_y": "home_team_name"}, axis=1
    )
    df_fixtures = df_fixtures.drop(["team_id_x", "team_id_y"], axis=1)

    # Add home/away representation
    df_fixtures = df_fixtures.rename(
        {
            "home_team_name": "home",
            "away_team_name": "away",
            "kickoff_time": "game_date",
            "event": "gameweek",
        },
        axis=1,
    )
    df_fixtures[["home", "away"]] = df_fixtures[["home", "away"]].apply(
        lambda col: col.str.strip(), axis=1
    )
    df_fixtures["game_date"] = pd.to_datetime(df_fixtures["game_date"]).dt.date
    df_fixtures["game_date"] = pd.to_datetime(
        df_fixtures["game_date"]
    )  # Weirdly this has to be doubled to isolate the date aspect as datetime instead of object :/
    df_fixtures = df_fixtures[["home", "away", "game_date", "gameweek"]]

    return df_fixtures


def fixtures_to_home_away(df_fixtures):
    fixtures_1 = df_fixtures[["home", "away", "game_date", "gameweek"]]
    fixtures_2 = df_fixtures[["home", "away", "game_date", "gameweek"]]
    fixtures_1["is_home"] = True
    fixtures_2["is_home"] = False
    fixtures_1 = fixtures_1.rename(
        {"home": "team_name", "away": "opponent_team_name"}, axis=1
    )
    fixtures_2 = fixtures_2.rename(
        {"away": "team_name", "home": "opponent_team_name"}, axis=1
    )
    fixtures_processed = pd.concat([fixtures_1, fixtures_2])
    return fixtures_processed


def process_fixtures(df_fixtures, df_team_ids):
    df_fixtures = add_team_names(df_fixtures, df_team_ids)
    df_fixtures = fixtures_to_home_away(df_fixtures)
    return df_fixtures


def get_gameweek_dates(df_fixtures, season):
    first_kickoff = df_fixtures.groupby("gameweek")[["game_date"]].first()
    first_kickoff = first_kickoff.rename(columns={"game_date": "start_first"})
    last_kickoff = df_fixtures.groupby("gameweek")[["game_date"]].last()
    last_kickoff = last_kickoff.rename(columns={"game_date": "start_last"})
    df_kickoff = pd.concat([first_kickoff, last_kickoff], axis=1).reset_index()
    df_kickoff["season"] = season
    return df_kickoff
