import pandas as pd
import lionel.data_load.process.process_fixtures as process_fixtures
from lionel.data_load.constants import PROCESSED, RAW, CLEANED


def run(season=24):

    # Fixtures
    df_fixtures = pd.read_csv(RAW / f"fixtures_{season}.csv")
    df_team_ids = pd.read_csv(CLEANED / f"team_ids_{season}.csv")
    df_fixtures = process_fixtures.process_fixtures(df_fixtures, df_team_ids)
    df_fixtures.to_csv(PROCESSED / f"fixtures_{season}.csv", index=False)

    # Gameweek dates
    df_gameweek_dates = process_fixtures.get_gameweek_dates(df_fixtures, season)
    df_gameweek_dates.to_csv(PROCESSED / f"gameweek_dates_{season}.csv", index=False)
    pass


if __name__ == "__main__":
    run()
