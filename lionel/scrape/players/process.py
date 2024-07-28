import pandas as pd
from scrape.players.scrape import FPLScraper


# TODO: This shouldn't be a class really, just a function
class FPLProcessor:
    def __init__(self, season, next_gameweek):

        # Inputs
        self.season = season
        self.gameweek = next_gameweek

        # Scraper
        self.scraper = FPLScraper(season)
        self.scraper.run_scrape()

        # Data
        self.fixtures = pd.DataFrame()
        self.player_stats = pd.DataFrame()  # This should be pushed to DB
        self.next_fixtures = pd.DataFrame()
        self.gameweek_dates = pd.DataFrame()

    @staticmethod
    def _process_fixtures(fixtures_raw):
        fixtures_1 = fixtures_raw[["home", "away", "game_date", "gameweek"]]
        fixtures_2 = fixtures_raw[["home", "away", "game_date", "gameweek"]]
        fixtures_1["is_home"] = True
        fixtures_2["is_home"] = False
        fixtures_1 = fixtures_1.rename(
            {"home": "team_name", "away": "next_opponent_name"}, axis=1
        )
        fixtures_2 = fixtures_2.rename(
            {"away": "team_name", "home": "next_opponent_name"}, axis=1
        )
        fixtures_processed = pd.concat([fixtures_1, fixtures_2])
        return fixtures_processed

    @property
    def fixtures(self):
        if self._fixtures.empty:
            fixtures_raw = self.scraper.fixtures
            fixtures = self._process_fixtures(fixtures_raw)
            self._fixtures = fixtures
        return self._fixtures

    @fixtures.setter
    def fixtures(self, f):
        self._fixtures = f

    def _collapse_past_games(self, games_window=30):
        """Collapse gameweek by gameweek stats until the latest gw"""

        current_gw = self.gameweek - 1
        next_gw = self.gameweek
        season = int(self.season)

        df = self.scraper.gw_stats
        df["season"] = df["season"].astype(int)

        df = df.reset_index(drop=True)
        if current_gw < 5:
            df = df[
                (
                    (df["GW"] < current_gw)
                    & (df["GW"] > current_gw - games_window)
                    & (df["season"] == season)
                )
                | (
                    (df["GW"] > current_gw + 38 - games_window)
                    & (df["season"] == season - 1)
                )
            ]
        else:
            df = df[(df["season"] == season)]

        df = df.groupby(["name"]).agg(
            {
                "total_points": "sum",
                "team_name": "last",
                "value": "last",
                "ict_index": "mean",
                "minutes": "sum",
                "position": "last",
                "team_id": "last",
                "element": "last",
            }
        )

        df["season"] = season
        df = df.reset_index()
        df["next_gw"] = next_gw
        return df

    def _sum_player_stats(self):

        fixtures_gw = self.fixtures[self.fixtures["gameweek"] == self.gameweek]
        gw_stats_collapsed = self._collapse_past_games(games_window=30)

        gw_stats_collapsed = gw_stats_collapsed.rename(columns={"team_name": "team"})
        players = gw_stats_collapsed.merge(
            fixtures_gw, left_on="team", right_on="team_name", how="left"
        )
        players = players.drop("team_name", axis=1)

        players = players.dropna()
        players = players.rename(columns={"team": "team_name"})

        return players

    @property
    def player_stats(self):
        if self._player_stats.empty:
            self._player_stats = self._sum_player_stats()
        return self._player_stats

    @player_stats.setter
    def player_stats(self, val):
        self._player_stats = val

    @property
    def next_fixtures(self):
        if self._next_fixtures.empty:
            self._next_fixtures = self.fixtures[
                self.fixtures["gameweek"] == self.gameweek
            ]
        return self._next_fixtures

    @next_fixtures.setter
    def next_fixtures(self, val):
        self._next_fixtures = val

    @property
    def gameweek_dates(self):
        if self._gameweek_dates.empty:
            first_kickoff = self.fixtures.groupby("gameweek")[["game_date"]].first()
            first_kickoff = first_kickoff.rename(columns={"game_date": "start_first"})
            last_kickoff = self.fixtures.groupby("gameweek")[["game_date"]].last()
            last_kickoff = last_kickoff.rename(columns={"game_date": "start_last"})
            df_kickoff = pd.concat([first_kickoff, last_kickoff], axis=1).reset_index()
            df_kickoff["season"] = self.season
            self._gameweek_dates = df_kickoff
        return self._gameweek_dates

    @gameweek_dates.setter
    def gameweek_dates(self, val):
        self._gameweek_dates = val
