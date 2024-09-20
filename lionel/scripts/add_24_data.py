from lionel.constants import DATA, RAW
from lionel.db.connector import DBManager
import pandas as pd
import numpy as np


def add_24_players(dbm):
    keep_cols = [
        "player_id_web",
        "web_name",
        "team_id_web",
        "position",
        "season",
        "player_season_id",
        "player_full_name",
    ]

    df_players_24 = pd.read_csv(RAW / "players_raw_24.csv")
    df_players_24 = df_players_24.rename(
        columns={"id": "player_id_web", "team": "team_id_web"}
    )
    position_map = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}
    df_players_24["position"] = df_players_24["element_type"].map(position_map)
    df_players_24["season"] = 24
    df_players_24["player_season_id"] = (
        df_players_24.season.astype(str) + df_players_24.player_id_web.astype(str)
    ).astype(int)
    df_players_24["player_full_name"] = (
        df_players_24["first_name"] + " " + df_players_24["second_name"]
    )
    df_players_24 = df_players_24[keep_cols]

    df_players_24.to_sql(
        "player_seasons_staging", dbm.engine, if_exists="append", index=False
    )

    # Move new players into main_players table
    q1 = """
    INSERT INTO players (player_name, player_full_name)
    SELECT web_name AS player_name, player_full_name 
    FROM player_seasons_staging 
    WHERE player_full_name NOT IN 
        (SELECT player_full_name FROM
        players) 
    """

    # Move player_seasons_staging into player_seasons, add player_id
    q2 = """
    INSERT INTO player_seasons
    SELECT * FROM
        (SELECT player_seasons_staging.player_season_id as player_season_id, players.player_id as player_id, 
            player_seasons_staging.player_id_web, player_seasons_staging.season, player_seasons_staging.web_name AS player_name,
            player_seasons_staging.player_full_name,
            (SELECT CAST(CONCAT(season, team_id_web) AS INT) FROM player_seasons_staging) AS team_season_id,
            position
        FROM players
        INNER JOIN player_seasons_staging ON players.player_full_name = player_seasons_staging.player_full_name)
    WHERE player_season_id NOT IN (
        SELECT DISTINCT player_season_id FROM player_seasons
    )
    """

    q3 = "DELETE FROM player_seasons_staging"
    for q in [q1, q2, q3]:
        dbm.query(q)

    return True


def add_24_teams(dbm):
    df_24 = pd.read_csv(DATA / "cleaned/team_ids_24.csv")
    df_24 = df_24.rename(columns={"team_id": "team_id_web"})
    df_24["team_season_id"] = (
        df_24["season"].astype(str) + "_" + df_24["team_id_web"].astype(str)
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
    df_24 = df_24[KEEP_COLS]
    df_24.to_sql(
        "teams_season_staging", con=dbm.engine, if_exists="append", index=False
    )

    q = """
        INSERT INTO teams (team_name)
        SELECT team_name 
        FROM teams_season_staging
        WHERE team_name NOT IN 
            (SELECT team_name FROM
            teams) 
    """
    q2 = """
        INSERT INTO teams_season (
            team_season_id, team_constant_id, team_name, team_id_web, 
            season, strength, strength_overall_home, strength_overall_away, 
            strength_attack_home, strength_attack_away, strength_defence_home, 
            strength_defence_away)
        SELECT * FROM
            (SELECT teams_season_staging.team_season_id, teams.team_id as team_constant_id, 
                teams_season_staging.team_name, teams_season_staging.team_id_web, teams_season_staging.season, 
                teams_season_staging.strength,
                teams_season_staging.strength_overall_home, teams_season_staging.strength_overall_away,
                teams_season_staging.strength_attack_home, teams_season_staging.strength_attack_away,
                teams_season_staging.strength_defence_home, teams_season_staging.strength_defence_away
            FROM teams
            INNER JOIN teams_season_staging ON teams.team_name = teams_season_staging.team_name)
        WHERE team_season_id NOT IN (
            SELECT DISTINCT team_season_id FROM teams_season
        )
    """
    # Empty the staging tables
    q3 = """
        DELETE FROM teams_season_staging
    """
    for q in [q, q2, q3]:
        dbm.query(q)


def add_24_player_stats(dbm):
    df_stats_24 = pd.read_csv(RAW / "player_stats_24.csv").rename(
        columns={"GW": "gameweek", "name": "player_full_name"}
    )
    df_stats_24["season"] = 24
    df_stats_24["gameweek_id"] = (
        df_stats_24["season"].astype(str) + df_stats_24["gameweek"].astype(str)
    ).astype(int)
    TEAM_MAP = {
        "team": {
            "Man City": "Manchester City",
            "Man Utd": "Manchester Utd",
            "Spurs": "Tottenham",
            "Nott'm Forest": "Nottingham",
        }
    }
    df_stats_24 = df_stats_24.replace(TEAM_MAP)

    # Add player_season_id
    df_players_24 = pd.read_csv(RAW / "players_raw_24_2.csv")
    df_stats_24_2 = df_stats_24.merge(
        df_players_24[["player_full_name", "player_season_id"]],
        on="player_full_name",
        how="left",
        indicator=True,
    )

    # Ignore these missing ones - three names not in the players_raw file
    missing = [
        "Michale Olakigbe",
        "Yegor Yarmolyuk",
        "Djordje Petrovic",
        "Max Kinsey-Wellings",
    ]
    assert (
        df_stats_24_2[df_stats_24_2.player_season_id.isnull()]
        .player_full_name.isin(missing)
        .all()
    )
    # df_stats_24_2[df_stats_24_2.player_season_id.isnull()]

    df_stats_24_2 = df_stats_24_2.loc[
        df_stats_24_2.player_season_id.notnull(),
        [col for col in df_stats_24_2.columns if col != "_merge"],
    ]

    # Merge fixture i
    fixtures_24 = pd.read_csv(RAW / "fixtures_24.csv")

    # Need to merge team id too....
    team_ids_24 = pd.read_csv(DATA / "cleaned/team_ids_24.csv")

    df_stats_24_2 = df_stats_24_2.merge(
        team_ids_24[["team_name", "team_id"]],
        left_on="team",
        right_on="team_name",
        how="left",
    )
    df_stats_24_2["team_h"] = np.where(
        df_stats_24_2["was_home"],
        df_stats_24_2["team_id"],
        df_stats_24_2["opponent_team"],
    )
    df_stats_24_2["team_a"] = np.where(
        df_stats_24_2["was_home"],
        df_stats_24_2["opponent_team"],
        df_stats_24_2["team_id"],
    )

    # df_stats_24_2
    df_stats_24_3 = df_stats_24_2.merge(
        fixtures_24[["team_h", "team_a", "id"]], on=["team_h", "team_a"], how="left"
    )
    df_stats_24_3 = df_stats_24_3.rename(columns={"id": "fixture_id"})
    df_stats_24_3["fixture_season_id"] = (
        df_stats_24_3["season"].astype(str) + df_stats_24_3["fixture_id"].astype(str)
    ).astype(int)
    df_stats_24_3["player_id_web"] = (
        df_stats_24_3["player_season_id"].astype(int).astype(str).str[2:].astype(int)
    )
    # df_stats_24_2._merge.value_counts()

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

    df_stats_24_3 = df_stats_24_3[
        [col for col in KEEP_COLS if col in df_stats_24_3.columns]
    ]
    df_stats_24_3["player_season_id"] = df_stats_24_3["player_season_id"].astype(int)
    df_stats_24_3["player_stat_id"] = (
        df_stats_24_3.season.astype(str) + df_stats_24_3.index.astype(str)
    ).astype(int)

    df_stats_24_3.to_sql(
        "player_stats_staging", dbm.engine, if_exists="append", index=False
    )

    # Add to main tables
    q = "DELETE FROM player_stats WHERE season = 24"
    q2 = """
    INSERT INTO player_stats
        SELECT * FROM
        (SELECT player_stats_staging.player_stat_id, player_stats_staging.player_season_id, player_seasons.player_id, 
        player_stats_staging.fixture_season_id,
        player_stats_staging.fixture_id,
        player_stats_staging.gameweek_id,
        player_stats_staging.gameweek,
        player_stats_staging.season,
        player_stats_staging.total_points,
        player_stats_staging.was_home,
        player_stats_staging.minutes,
        player_stats_staging.goals_scored,
        player_stats_staging.assists,
        player_stats_staging.clean_sheets,
        player_stats_staging.goals_conceded,
        player_stats_staging.own_goals,
        player_stats_staging.penalties_saved,
        player_stats_staging.penalties_missed,
        player_stats_staging.yellow_cards,
        player_stats_staging.red_cards,
        player_stats_staging.saves,
        player_stats_staging.bonus,
        player_stats_staging.bps,
        player_stats_staging.influence,
        player_stats_staging.creativity,
        player_stats_staging.threat,
        player_stats_staging.ict_index,
        player_stats_staging.expected_goals,
        player_stats_staging.expected_assists,
        player_stats_staging.expected_goal_involvements,
        player_stats_staging.expected_goals_conceded,
        player_stats_staging.value,
        player_stats_staging.transfers_balance,
        player_stats_staging.selected,
        player_stats_staging.transfers_in,
        player_stats_staging.transfers_out
        FROM player_seasons 
        INNER JOIN player_stats_staging     
            ON player_seasons.player_season_id = player_stats_staging.player_season_id )
    """
    q3 = "DELETE FROM player_stats_staging"
    dbm.query(q)
    dbm.query(q2)
    dbm.query(q3)


def add_24_fixtures(dbm):
    fixtures_24 = pd.read_csv(RAW / "fixtures_24.csv")
    fixtures_24 = fixtures_24.rename(
        columns={
            "id": "fixture_id",
            "event": "gameweek",
            "team_h": "team_h_id",
            "team_a": "team_a_id",
        }
    )
    fixtures_24["season"] = 24
    fixtures_24["fixture_season_id"] = (
        fixtures_24["season"].astype(str) + fixtures_24["fixture_id"].astype(str)
    ).astype(int)
    fixtures_24["gameweek_id"] = (
        fixtures_24["season"].astype(str) + fixtures_24["gameweek"].astype(str)
    ).astype(int)

    fixtures_24["team_h_season_id"] = (
        fixtures_24["season"].astype(str) + fixtures_24["team_h_id"].astype(str)
    ).astype(int)
    fixtures_24["team_a_season_id"] = (
        fixtures_24["season"].astype(str) + fixtures_24["team_a_id"].astype(str)
    ).astype(int)

    KEEP_COLS = [
        "gameweek",
        "finished",
        "fixture_id",
        "kickoff_time",
        "team_a_score",
        "team_h_score",
        "season",
        "fixture_season_id",
        "gameweek_id",
        "team_a_season_id",
        "team_h_season_id",
    ]
    fixtures_24 = fixtures_24[[col for col in KEEP_COLS if col in fixtures_24.columns]]
    fixtures_24.to_sql("fixtures_staging", dbm.engine, if_exists="append", index=False)
    q = "DELETE FROM fixtures WHERE season = 24"
    q2 = """
        INSERT INTO fixtures
        SELECT * FROM fixtures_staging
        WHERE season = 24
    """
    q3 = "DELETE FROM fixtures_staging"
    dbm.query(q)
    dbm.query(q2)
    dbm.query(q3)


def add_24_data(dbm):
    add_24_teams(dbm)
    add_24_players(dbm)
    add_24_fixtures(dbm)
    add_24_player_stats(dbm)
    return True


if __name__ == "__main__":
    dbm = DBManager(DATA / "fpl.db")
    add_24_data(dbm)
    print("Data added to db")
