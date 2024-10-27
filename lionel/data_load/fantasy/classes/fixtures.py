import copy

import pandas as pd

from lionel.constants import RAW
from lionel.data_load.fantasy.base_class import AbstractLoader
from lionel.data_load.fantasy.config import DataLoadConfig as Config


class FixtureLoader(AbstractLoader):

    @property
    def key(self):
        return "fixtures"

    def read_from_file(self, season):
        return super().read_from_file(RAW / f"fixtures_raw_{season}.csv")

    def read_from_scrape(self):
        self.data = pd.DataFrame(self._read_from_scrape())
        return self.data

    def _clean_fixtures(self):
        assert self.data, "Load the data first"
        df = copy.copy(self.data)

        df["kickoff_time"] = pd.to_datetime(df["kickoff_time"]).dt.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        df = df.rename(columns=Config.FIXTURE_COL_MAP)[
            [
                "gameweek",
                "home_id",
                "away_id",
                "home_score",
                "away_score",
                "kickoff_time",
                "fixture_season_id",
            ]
        ]
        return df

    def _add_team_ids(self, df, season):
        team_seasons = pd.read_sql(
            f"SELECT web_id, team_id FROM team_seasons WHERE season={season}",
            self.db_manager.engine.raw_connection(),
        )

        # Merge team IDs on home/away
        df = (
            df.merge(team_seasons, left_on="home_id", right_on="web_id")
            .drop(columns=["web_id", "home_id"])
            .rename(columns={"team_id": "home_id"})
            .merge(team_seasons, left_on="away_id", right_on="web_id")
            .drop(columns=["web_id", "away_id"])
            .rename(columns={"team_id": "away_id"})
        )
        return df

    def _add_gameweek_ids(self, df, season):
        gameweeks = pd.read_sql(
            f"SELECT id, gameweek FROM gameweeks WHERE season = {season}",
            self.db_manager.engine.raw_connection(),
        )
        df = df.merge(gameweeks, left_on="gameweek", right_on="gameweek").rename(
            columns=Config.GW_COL_MAP
        )
        return df

    def process(self, season):
        assert self.data, "Load the data first"
        df = self._clean_fixtures()
        df = self._add_team_ids(df, season)
        df = self._add_gameweek_ids(df, season)
        return df

    def run(self, season):
        df = self.process(season)
        self._delete_and_load_to_db(df, season, "fixtures")
        return True
