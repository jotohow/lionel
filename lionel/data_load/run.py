from lionel.data_load.constants import RAW, CLEANED, PROCESSED
import lionel.data_load.ingestion.fpl.extract_fixtures as extract_fixtures
import lionel.data_load.ingestion.fpl.extract_team_ids as extract_team_ids
import lionel.data_load.ingestion.fpl.extract_player_stats as extract_player_stats
import lionel.data_load.process.process_fixtures as process_fixtures
import lionel.data_load.storage.storage_handler as storage_handler


def run_ingestion(storage_handler, season=24):
    df_gw_stats = extract_player_stats.get_gw_stats(season)
    storage_handler.store(df_gw_stats, RAW / f"gw_stats_{season}.csv", index=False)
    df_gw_stats = extract_player_stats.clean_gw_stats(df_gw_stats)
    storage_handler.store(df_gw_stats, CLEANED / f"gw_stats_{season}.csv", index=False)

    extract_fixtures.update_local_fixtures(season)

    df_team_ids = extract_team_ids.get_team_ids(season)
    storage_handler.store(df_team_ids, RAW / f"team_ids_{season}.csv", index=False)

    df_team_ids = extract_team_ids.clean_team_ids(df_team_ids)
    storage_handler.store(df_team_ids, CLEANED / f"team_ids_{season}.csv", index=False)


def run_processing(storage_handler, season=24):
    # Fixtures
    df_fixtures = storage_handler.load(RAW / f"fixtures_{season}.csv")
    df_team_ids = storage_handler.load(CLEANED / f"team_ids_{season}.csv")
    df_fixtures = process_fixtures.process_fixtures(df_fixtures, df_team_ids)
    storage_handler.store(
        df_fixtures, PROCESSED / f"fixtures_{season}.csv", index=False
    )

    # Gameweek dates
    df_gameweek_dates = process_fixtures.get_gameweek_dates(df_fixtures, season)
    storage_handler.store(
        df_gameweek_dates, PROCESSED / f"gameweek_dates_{season}.csv", index=False
    )


if __name__ == "__main__":
    storage_handler = storage_handler.StorageHandler(local=True)
    run_ingestion(storage_handler)
    run_processing(storage_handler)
