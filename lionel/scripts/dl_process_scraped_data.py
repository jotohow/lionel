import pandas as pd


def clean_teams(data, season=25):
    TEAM_MAP = {
        "team_name": {
            "Man City": "Manchester City",
            "Man Utd": "Manchester Utd",
            "Spurs": "Tottenham",
            "Nott'm Forest": "Nottingham",
        }
    }
    # Note this will fail if there's already data in the table...
    df_teams = (
        pd.DataFrame.from_dict(data["team_map"], orient="index")
        .reset_index()
        .rename(columns={"index": "team_id_web", "name": "team_name"})
    ).replace(TEAM_MAP)
    df_teams["season"] = season
    df_teams["team_season_id"] = (
        df_teams.season.astype(str) + df_teams.team_id_web.astype(str)
    ).astype(int)
    KEEP_COLS = [
        "team_season_id",
        "team_id_web",
        "team_name",
        "season",
        "strength",
        "strength_overall_home",
        "strength_overall_away",
        "strength_attack_home",
        "strength_attack_away",
        "strength_defence_home",
        "strength_defence_away",
    ]
    return df_teams[KEEP_COLS]


def clean_fixtures(data, season=25):
    df_fixtures = (
        pd.DataFrame(data["fixtures"])
        .rename(
            columns={
                "id": "fixture_id",
                "team_a": "team_a_id",
                "team_h": "team_h_id",
                "event": "gameweek",
            }
        )
        .drop(columns=["team_a_difficulty", "team_h_difficulty"])
    )
    df_fixtures["kickoff_time"] = pd.to_datetime(df_fixtures["kickoff_time"])
    df_fixtures["season"] = season
    df_fixtures["fixture_season_id"] = (
        df_fixtures.season.astype(str) + df_fixtures.fixture_id.astype(str)
    ).astype(int)
    df_fixtures["gameweek_id"] = (
        df_fixtures.season.astype(str) + df_fixtures.gameweek.astype(str)
    ).astype(int)
    df_fixtures["team_a_season_id"] = (
        df_fixtures["season"].astype(str) + df_fixtures["team_a_id"].astype(str)
    ).astype(int)
    df_fixtures["team_h_season_id"] = (
        df_fixtures["season"].astype(str) + df_fixtures["team_h_id"].astype(str)
    ).astype(int)
    df_fixtures = df_fixtures.drop(columns=["team_a_id", "team_h_id"])
    return df_fixtures


def clean_gameweeks(data, season=25):
    df_gameweeks = (
        pd.DataFrame.from_dict(data["gw_deadlines"], orient="index")
        .reset_index()
        .rename(columns={"index": "gameweek", 0: "deadline_time"})
    )
    df_gameweeks["season"] = season
    df_gameweeks["gameweek_id"] = (
        df_gameweeks.season.astype(str) + df_gameweeks.gameweek.astype(str)
    ).astype(int)
    df_gameweeks["deadline_time"] = pd.to_datetime(df_gameweeks["deadline_time"])
    return df_gameweeks


def clean_players(data, season=25):
    df_players = pd.DataFrame.from_dict(data["element_map"], orient="index").rename(
        columns={"id": "player_id_web", "team_id": "team_id_web"}
    )
    position_map = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}
    df_players["position"] = df_players["element_type"].map(position_map)
    df_players["season"] = season
    df_players["player_season_id"] = (
        df_players.season.astype(str) + df_players.player_id_web.astype(str)
    ).astype(int)

    # Some player names to adjust here - Solanke being the obvious one

    df_players = df_players.drop(columns=["element_type"])
    df_players["player_full_name"] = (
        df_players["first_name"] + " " + df_players["second_name"]
    )

    df_players = df_players.replace(
        {
            "player_full_name": {"Dominic Solanke-Mitchell": "Dominic Solanke"},
            "player_name": {
                "Philip": "Billing",
                "J.Ramsey": "Ramsey",
                "Cook": "L.Cook",
                "H.Traorè": "Hamed Traorè",
                "Kozłowski": "Kozlowski",
                "Sánchez": "Sanchez",
                "Andrey Santos": "Andrey",
                "Vinicius": "Vinícius",
                "N.Phillips": "Phillips",
                "M.Salah": "Salah",
                "N.Willians": "Williams",
                "P.M.Sarr": "Sarr",
                "J.Gomes": "João Gomes",
            },
        }
    )

    df_players = df_players.drop(columns=["first_name", "second_name"])
    return df_players


def clean_player_stats(data, season=25):
    player_keys = [data[k] for k in data.keys() if "player_stats_" in k]
    player_keys = [x for xs in player_keys for x in xs]
    df_player_stats = pd.DataFrame(player_keys)
    df_player_stats["season"] = season
    df_player_stats = df_player_stats.rename(
        columns={
            "element": "player_id_web",
            "round": "gameweek",
            "fixture": "fixture_id",
        }
    )
    df_player_stats["gameweek_id"] = (
        df_player_stats.season.astype(str) + df_player_stats.gameweek.astype(str)
    ).astype(int)
    df_player_stats["fixture_season_id"] = (
        df_player_stats.season.astype(str) + df_player_stats.fixture_id.astype(str)
    ).astype(int)
    df_player_stats["player_stat_id"] = (
        df_player_stats.season.astype(str) + df_player_stats.index.astype(str)
    ).astype(int)
    df_player_stats["player_season_id"] = (
        df_player_stats.season.astype(str) + df_player_stats.player_id_web.astype(str)
    ).astype(int)
    KEEP_COLS = [
        "player_stat_id",
        "player_season_id",
        "fixture_id",
        "fixture_season_id",
        "gameweek",
        "gameweek_id",
        "season",
        "total_points",
        "was_home",
        "minutes",
        "goals_scored",
        "assists",
        "clean_sheets",
        "goals_conceded",
        "own_goals",
        "penalties_saved",
        "penalties_missed",
        "yellow_cards",
        "red_cards",
        "saves",
        "bonus",
        "bps",
        "influence",
        "creativity",
        "threat",
        "ict_index",
        "expected_goals",
        "expected_assists",
        "expected_goal_involvements",
        "expected_goals_conceded",
        "value",
        "transfers_balance",
        "selected",
        "transfers_in",
        "transfers_out",
    ]
    return df_player_stats[KEEP_COLS]
