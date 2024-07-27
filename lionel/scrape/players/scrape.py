import pandas as pd


class FPLScraper:

    BASE_URL = (
        "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"
    )
    SEASON_MAP = {
        24: "2023-24",
        23: "2022-23",
        22: "2021-22",
        21: "2020-21",
        20: "2019-20",
        19: "2018-19",
    }

    def __init__(self, season):
        self.season = season
        self.team_ids = pd.DataFrame()
        self.fixtures = pd.DataFrame()
        self.gw_stats = pd.DataFrame()

    @property
    def team_ids(self):
        if self._team_ids.empty:
            team_ids = pd.read_csv(
                f"{FPLScraper.BASE_URL}/{FPLScraper.SEASON_MAP[self.season]}/teams.csv"
            )
            team_ids = team_ids.rename(columns={"name": "team", "id": "team_id"})
            team_ids = team_ids.replace(
                {
                    "team": {
                        "Man City": "Manchester City",
                        "Man Utd": "Manchester Utd",
                        "Spurs": "Tottenham",
                        "Nott'm Forest": "Nottingham",
                    }
                }
            )
            self._team_ids = team_ids
        return self._team_ids

    @team_ids.setter
    def team_ids(self, val):
        self._team_ids = val

    def _format_fixtures(self, fixtures):
        fixtures = fixtures.merge(
            self.team_ids, left_on="team_a", right_on="team_id", how="left"
        )
        fixtures = fixtures.merge(
            self.team_ids, left_on="team_h", right_on="team_id", how="left"
        )

        # Cleaning
        fixtures = fixtures.rename(
            {"team_x": "away_team_name", "team_y": "home_team_name"}, axis=1
        )
        fixtures = fixtures.drop(["team_id_x", "team_id_y"], axis=1)

        # Add home/away representation
        fixtures = fixtures.rename(
            {
                "home_team_name": "home",
                "away_team_name": "away",
                "kickoff_time": "game_date",
                "event": "gameweek",
            },
            axis=1,
        )
        fixtures[["home", "away"]] = fixtures[["home", "away"]].apply(
            lambda col: col.str.strip(), axis=1
        )
        fixtures["game_date"] = pd.to_datetime(fixtures["game_date"]).dt.date
        fixtures["game_date"] = pd.to_datetime(
            fixtures["game_date"]
        )  # Weirdly this has to be doubled to isolate the date aspect as datetime instead of object :/
        fixtures = fixtures[["home", "away", "game_date", "gameweek"]]

        return fixtures

    @property
    def fixtures(self):
        if self._fixtures.empty:
            fixtures = pd.read_csv(
                f"{FPLScraper.BASE_URL}/{FPLScraper.SEASON_MAP[self.season]}/fixtures.csv"
            )
            self._fixtures = self._format_fixtures(fixtures)
        return self._fixtures

    @fixtures.setter
    def fixtures(self, val):
        self._fixtures = val

    def _get_gw_stats(self):
        gw_stats_current = pd.read_csv(
            f"{FPLScraper.BASE_URL}/{FPLScraper.SEASON_MAP[self.season]}/gws/merged_gw.csv"
        )
        gw_stats_previous = pd.read_csv(
            f"{FPLScraper.BASE_URL}/{FPLScraper.SEASON_MAP[self.season-1]}/gws/merged_gw.csv"
        )

        gw_stats_current["season"] = self.season
        gw_stats_previous["season"] = self.season - 1

        gw_stats = pd.concat([gw_stats_current, gw_stats_previous])
        gw_stats["team_id"] = 99
        gw_stats = gw_stats.rename(
            columns={"team": "team_name", "GW": "gameweek", "kickoff_time": "game_date"}
        )

        MAP_ = {
            "team_name": {
                "Man City": "Manchester City",
                "Man Utd": "Manchester Utd",
                "Spurs": "Tottenham",
                "Nott'm Forest": "Nottingham",
            },
            "name": {
                "Son Heung-min": "Heung-Min Son",
                "João Cancelo": "João Pedro Cavaco Cancelo",
                "Emerson Leite de Souza Junior": "Emerson Aparecido Leite de Souza Junior",
            },
        }

        gw_stats = gw_stats.replace(MAP_)
        gw_stats["game_date"] = pd.to_datetime(gw_stats["game_date"]).dt.date
        gw_stats["game_date"] = pd.to_datetime(gw_stats["game_date"])
        # gw_stats = gw_stats.rename(columns={'team_name': 'team' })  # ADDED from Team._sum_player_stats()
        NEEDED_COLS = [
            "gameweek",
            "assists",
            "bps",
            "creativity",
            "element",
            "goals_scored",
            "ict_index",
            "influence",
            "minutes",
            "name",
            "opponent_team",
            "position",
            "season",
            "selected",
            "team_a_score",
            "team_h_score",
            "team_id",
            "team_name",
            "threat",
            "total_points",
            "transfers_balance",
            "value",
            "was_home",
            "game_date",
        ]

        gw_stats = gw_stats[[col for col in gw_stats.columns if col in NEEDED_COLS]]
        return gw_stats

    @property
    def gw_stats(self):
        if self._gw_stats.empty:
            raise Exception("Run the scrape first")
        return self._gw_stats

    @gw_stats.setter
    def gw_stats(self, val):
        self._gw_stats = val

    def run_scrape(self):
        self.gw_stats = self._get_gw_stats()
        return self.gw_stats
