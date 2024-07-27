from lionel.data_load.constants import RAW, CLEANED
import lionel.data_load.ingestion.fpl.extract_fixtures as extract_fixtures
import lionel.data_load.ingestion.fpl.extract_team_ids as extract_team_ids
import lionel.data_load.ingestion.fpl.extract_player_stats as extract_player_stats


def run(season=24):
    df_gw_stats = extract_player_stats.get_gw_stats(season)
    df_gw_stats.to_csv(RAW / f"gw_stats_{season}.csv", index=False)
    df_gw_stats = extract_player_stats.clean_gw_stats(df_gw_stats)
    df_gw_stats.to_csv(CLEANED / f"gw_stats_{season}.csv", index=False)

    extract_fixtures.update_local_fixtures(season)

    df_team_ids = extract_team_ids.get_team_ids(season)
    df_team_ids.to_csv(RAW / f"team_ids_{season}.csv", index=False)
    df_team_ids = extract_team_ids.clean_team_ids(df_team_ids)
    df_team_ids.to_csv(CLEANED / f"team_ids_{season}.csv", index=False)


if __name__ == "__main__":
    run()
