import pandas as pd
import numpy as np
import datetime as dt

pd.options.mode.chained_assignment = None  # default='warn'


class Game:
    """
    Parses a game from the API response
    """

    def __init__(self, game_dict: dict):
        self.game_dict = game_dict
        self.bookmakers = []
        self.raw_odds = pd.DataFrame()
        self.adjusted_odds = pd.DataFrame()
        self.aggregated_odds = {}  # TODO
        self.home_team = ""
        self.away_team = ""
        self.date = dt.date
        self.game_date = None

    def __repr__(self):
        return f"Game object: {self.home_team} v {self.away_team} on {self.game_date}"

    @property
    def home_team(self):
        if not self._home_team:
            self._home_team = self.game_dict["home_team"]
        return self._home_team

    @home_team.setter
    def home_team(self, val):
        self._home_team = val

    @property
    def away_team(self):
        if not self._away_team:
            self._away_team = self.game_dict["away_team"]
        return self._away_team

    @away_team.setter
    def away_team(self, val):
        self._away_team = val

    @property
    def bookmakers(self):
        if self._bookmakers == []:
            self._bookmakers = self.game_dict["bookmakers"]
        return self._bookmakers

    @bookmakers.setter
    def bookmakers(self, val):
        self._bookmakers = val

    @property
    def game_date(self):
        if self._game_date is None:
            self._game_date = dt.datetime.strptime(
                self.game_dict["commence_time"], "%Y-%m-%dT%H:%M:%SZ"
            ).date()
        return self._game_date

    @game_date.setter
    def game_date(self, val):
        self._game_date = val

    @property
    def raw_odds(self):
        if self._raw_odds.empty:
            formatted_bookies = [
                self._format_one_bookies_data(bookie, self.home_team, self.away_team)
                for bookie in self.bookmakers
            ]
            self._raw_odds = pd.DataFrame.from_dict(formatted_bookies)
        return self._raw_odds

    @raw_odds.setter
    def raw_odds(self, val):
        self._raw_odds = val

    @property
    def adjusted_odds(self):
        if self._adjusted_odds.empty:
            self._adjusted_odds = self.raw_odds.apply(
                lambda row: self._adjust_raw_game_odds_for_margin(row), axis=1
            )
        return self._adjusted_odds

    @adjusted_odds.setter
    def adjusted_odds(self, val):
        self._adjusted_odds = val

    @staticmethod
    def _get_odds_for_one_bookie(odds_list, home_team, away_team):
        home_odds = [1 / d["price"] for d in odds_list if d["name"] == home_team][0]
        away_odds = [1 / d["price"] for d in odds_list if d["name"] == away_team][0]
        draw_odds = [1 / d["price"] for d in odds_list if d["name"] == "Draw"][0]
        return {"home_odds": home_odds, "away_odds": away_odds, "draw_odds": draw_odds}

    def _format_one_bookies_data(self, bookie, home_team, away_team):
        name = bookie["key"]
        update = bookie["last_update"]
        odds_list = bookie["markets"][0]["outcomes"]
        odds_dict = self._get_odds_for_one_bookie(odds_list, home_team, away_team)

        out = {
            "home_team": home_team,
            "away_team": away_team,
            "bookmaker": name,
            "updated_at": update,
        }
        out.update(odds_dict)
        return out

    @staticmethod
    def _adjust_raw_game_odds_for_margin(raw_odds: pd.Series) -> pd.Series:

        total_odds = (
            raw_odds["home_odds"] + raw_odds["away_odds"] + raw_odds["draw_odds"]
        )

        raw_odds["home_odds"] = raw_odds["home_odds"] / total_odds
        raw_odds["away_odds"] = raw_odds["away_odds"] / total_odds
        raw_odds["draw_odds"] = raw_odds["draw_odds"] / total_odds
        return raw_odds

    @property
    def aggregated_odds(self):
        if self._aggregated_odds == {}:
            home_odds = np.mean(self.adjusted_odds["home_odds"])
            away_odds = np.mean(self.adjusted_odds["away_odds"])
            draw_odds = np.mean(self.adjusted_odds["draw_odds"])
            self._aggregated_odds = {
                "home_odds": home_odds,
                "away_odds": away_odds,
                "draw_odds": draw_odds,
            }
        return self._aggregated_odds

    @aggregated_odds.setter
    def aggregated_odds(self, val):
        self._aggregated_odds = val

    def to_dict(self):
        dict_ = {"home_team": self.home_team, "away_team": self.away_team}
        dict_.update(self.aggregated_odds)
        return dict_
