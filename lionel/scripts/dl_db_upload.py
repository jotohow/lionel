import datetime as dt
import json
import sys

import lionel.scripts.dl_process_scraped_data as process
from lionel.constants import DATA, RAW
from lionel.db.connector import DBManager
from lionel.utils import setup_logger

logger = setup_logger(__name__)


def load_scraped_data(date=None):
    date = date or dt.datetime.today().strftime("%Y%m%d")
    with open(RAW / f"scraped_data_{date}.json", "r") as f:
        data = json.load(f)
    return data


def stage_data(data, dbmanager, season=25):
    logger.info("Staging data...")

    # Clean each set of data
    df_teams = process.clean_teams(data)
    df_fixtures = process.clean_fixtures(data)
    df_gameweeks = process.clean_gameweeks(data)
    df_players = process.clean_players(data)
    df_player_stats = process.clean_player_stats(data)

    pairs = [
        (df_teams, "teams_season_staging"),
        (df_fixtures, "fixtures_staging"),
        (df_gameweeks, "gameweeks_staging"),
        (df_players, "player_seasons_staging"),
        (df_player_stats, "player_stats_staging"),
    ]

    for tuple_ in pairs:
        df, table = tuple_
        dbmanager.delete_rows(table, season)
        df.to_sql(
            table,
            con=dbmanager.engine.raw_connection(),  # changed for pandas 2.2 - weird
            if_exists="append",
            index=False,
        )

    return True


def _move_teams(dbmanager, season=25):
    q1 = """
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
    q3 = "DELETE FROM teams_season_staging"
    for q in [q1, q2, q3]:
        dbmanager.query(q)

    return None


def _move_players(dbmanager):
    q1 = """
        INSERT INTO players (player_name, player_full_name)
        SELECT web_name AS player_name, player_full_name 
        FROM player_seasons_staging 
        WHERE player_full_name NOT IN 
            (SELECT player_full_name FROM
            players) 
    """

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
        dbmanager.query(q)


def _move_player_stats(dbmanager, season=25):
    dbmanager.delete_rows("player_stats", season)
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
    dbmanager.query(q2)


def _move_fixtures(dbmanager, season=25):
    dbmanager.delete_rows("fixtures", season)
    q = """INSERT INTO fixtures SELECT * FROM fixtures_staging"""
    dbmanager.query(q)
    dbmanager.delete_rows("fixtures_staging", season)


def _move_gameweeks(dbmanager, season=25):
    dbmanager.delete_rows("gameweeks", season)
    q = """INSERT INTO gameweeks SELECT * FROM gameweeks_staging"""
    dbmanager.query(q)
    dbmanager.delete_rows("gameweeks_staging", season)


def move_data_to_main(dbmanager, season=25):
    logger.info(f"Moving season {season} data from staging to main...")
    _move_teams(dbmanager)
    _move_players(dbmanager)
    _move_fixtures(dbmanager, season)
    _move_gameweeks(dbmanager, season)
    _move_player_stats(dbmanager, season)


def run(season=25):
    dbmanager = DBManager(DATA / "fpl.db")
    data = load_scraped_data()
    stage_data(data, dbmanager, season=season)
    move_data_to_main(dbmanager, season=season)

    return True


if __name__ == "__main__":
    run(season=int(sys.argv[1]))
