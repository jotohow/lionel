import json
import datetime as dt
from tqdm import tqdm
import sys
from pathlib import Path

# import lionel.data_load.ingestion.fpl.scraper_historic.extract_fixtures as extract_fixtures
# import lionel.data_load.ingestion.fpl.scraper_historic.extract_team_ids as extract_team_ids
# import lionel.data_load.ingestion.fpl.scraper_historic.extract_player_stats as extract_player_stats
# import lionel.data_load.process.process_fixtures as process_fixtures
# import lionel.data_load.process.process_player_stats as process_player_stats
# import lionel.data_load.process.process_train_data as process_train_data
import lionel.scripts.run_pipeline as run_pipeline
import lionel.data_load.db.init_db as init_db
from lionel.data_load.constants import RAW, DATA
from lionel.utils import setup_logger
from scrapl.fpl.runner import run_scrapers


logger = setup_logger(__name__)


# def run_ingestion(sh, season=24):
#     """
#     Run the data ingestion process.

#     Args:
#         storage_handler (StorageHandler): The storage handler object.
#         season (int): The season number.

#     Returns:
#         None
#     """
#     # Player stats
#     df_gw_stats = extract_player_stats.get_gw_stats(season)
#     sh.store(df_gw_stats, f"raw/gw_stats_{season}.csv", index=False)
#     df_gw_stats = extract_player_stats.clean_gw_stats(df_gw_stats)
#     sh.store(df_gw_stats, f"cleaned/gw_stats_{season}.csv", index=False)

#     df_cleaned_stats = extract_player_stats.get_clean_player_stats(season)
#     sh.store(df_cleaned_stats, f"raw/cleaned_players_{season}.csv", index=False)

#     # Fixtures
#     df_fixtures = extract_fixtures.get_fixtures(season)
#     sh.store(df_fixtures, f"raw/fixtures_{season}.csv", index=False)

#     # Team IDs
#     df_team_ids = extract_team_ids.get_team_ids(season)
#     sh.store(df_team_ids, f"raw/team_ids_{season}.csv", index=False)
#     df_team_ids = extract_team_ids.clean_team_ids(df_team_ids)
#     sh.store(df_team_ids, f"cleaned/team_ids_{season}.csv", index=False)


# def run_processing(sh, season=24):
#     """
#     Run the data processing process.

#     Args:
#         storage_handler (StorageHandler): The storage handler object.
#         season (int): The season number.

#     Returns:
#         None
#     """
#     df_team_ids = sh.load(f"cleaned/team_ids_{season}.csv")

#     # Fixtures
#     df_fixtures = sh.load(f"raw/fixtures_{season}.csv")
#     df_fixtures = process_fixtures.process_fixtures(df_fixtures, df_team_ids)
#     sh.store(df_fixtures, f"processed/fixtures_{season}.csv", index=False)

#     # Player stats
#     df_gw_stats = sh.load(f"cleaned/gw_stats_{season}.csv")
#     df_gw_stats = process_player_stats.add_opponent_names(df_gw_stats, df_team_ids)
#     sh.store(df_gw_stats, f"processed/gw_stats_{season}.csv", index=False)

#     # Gameweek dates
#     df_gameweek_dates = process_fixtures.get_gameweek_dates(df_fixtures, season)
#     sh.store(df_gameweek_dates, f"processed/gameweek_dates_{season}.csv", index=False)


# def run_analysis_data(sh, next_gw, season=24):
#     """
#     Run the analysis data process.

#     Args:
#         storage_handler (StorageHandler): The storage handler object.
#         next_gw (int): The next gameweek number.
#         season (int): The season number.

#     Returns:
#         df_train (DataFrame): The analysis data.
#     """
#     df_train = process_train_data.run(sh, next_gw, season=season)
#     sh.store(df_train, f"analysis/train_{next_gw}_{season}.csv", index=False)
#     return df_train


def run(season, elements=[]):
    # Init the database if needed
    if not Path(DATA / "fpl.db").exists():
        init_db.main()

    # Run scrapers if needed
    today = dt.datetime.today().strftime("%Y%m%d")
    p = RAW / f"scraped_data_{today}.json"
    if not p.exists():
        data = run_scrapers(elements)
        json.dump(data, open(p, "w"))

    run_pipeline.run(season=season)
    return True


if __name__ == "__main__":

    try:
        season = int(sys.argv[1])
    except IndexError:
        season = 25
    run(season)
