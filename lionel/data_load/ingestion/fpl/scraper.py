import requests
import pandas as pd
import json
import datetime as dt
from abc import ABC, abstractmethod
from tenacity import retry, stop_after_attempt, wait_fixed


class FPLScraperBase(ABC):

    def __init__(self):
        self.response_data = None
        self.scraped_data = {}
        self.scraped = False

    @property
    @abstractmethod
    def url(self):
        pass

    @abstractmethod
    def scrape(self) -> dict:
        pass

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
    def get_response(self, url):
        r = requests.get(url)
        assert r.ok
        d = r.json()
        self.response_data = d
        return d

    def to_json(self):  # do i want this to be at this levevl
        pass


class GenInfoScraper(FPLScraperBase):

    url = "https://fantasy.premierleague.com/api/bootstrap-static/"

    def __init__(self):
        super().__init__()
        self.team_map = None
        self.gw_deadlines = None
        self.element_map = None

    def scrape(self):
        d = self.get_response(self.url)
        self.scraped_data["team_map"] = self.get_team_map()
        self.scraped_data["gw_deadlines"] = self.get_gw_deadlines()
        self.scraped_data["element_map"] = self.get_element_name_map()
        self.scraped = True
        return self.scraped_data

    # TODO: Will need to clean the team names
    # because e.g. Nott'm Forest is not a good name
    def get_team_map(self):
        team_map = {
            team["id"]: {
                "name": team["name"],
                "strength": team["strength"],
                "strength_overall_home": team["strength_overall_home"],
                "strength_overall_away": team["strength_overall_away"],
                "strength_attack_home": team["strength_attack_home"],
                "strength_attack_away": team["strength_attack_away"],
                "strength_defence_home": team["strength_defence_home"],
                "strength_defence_away": team["strength_defence_away"],
            }
            for team in self.response_data["teams"]
        }
        self.team_map = team_map
        return team_map

    def get_gw_deadlines(self):
        gw_deadlines = {
            gw["id"]: gw["deadline_time"] for gw in self.response_data["events"]
        }
        # self.gw_deadlines = gw_deadlines
        return gw_deadlines

    def get_element_name_map(self):
        el = self.response_data["elements"]
        element_name_map = {
            el[i]["id"]: {
                "id": el[i]["id"],
                "web_name": el[i]["web_name"],
                "first_name": el[i]["first_name"],
                "second_name": el[i]["second_name"],
                "team_id": el[i]["team"],
                "element_type": el[i]["element_type"],
            }
            for i in range(len(el))
        }
        # self.element_map = element_name_map
        return element_name_map


class FixtureScraper(FPLScraperBase):

    url = "https://fantasy.premierleague.com/api/fixtures/"

    def __init__(self):
        super().__init__()
        self.fixture_data = None

    def scrape(self):
        d = self.get_response(self.url)
        fixture_data = self.parse_fixtures(d)
        self.scraped = True
        self.scraped_data["fixtures"] = fixture_data
        return self.scraped_data

    @staticmethod
    def parse_fixtures(data):
        keepkeys = [
            "event",
            "finished",
            "id",
            "kickoff_time",
            "team_a",
            "team_h",
            "team_a_difficulty",
            "team_h_difficulty",
            "team_a_score",
            "team_h_score",
        ]
        fixture_data = [{key: dict_[key] for key in keepkeys} for dict_ in data]
        return fixture_data


class GameweekScraper(FPLScraperBase):

    URL_BASE = "https://fantasy.premierleague.com/api/event/{GW}/live/"

    def __init__(self, gameweek):
        super().__init__()
        self.gameweek = gameweek
        self.url = self.URL_BASE.format(GW=gameweek)
        # self.gameweek_stats = []

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, url):
        self._url = url

    def scrape(self):
        d = self.get_response(self.url)
        stats = self.parse_gameweek_stats()
        self.scraped_data[f"gw_stats_{self.gameweek}"] = stats
        self.scraped = True
        return self.scraped_data

    # TODO: Need to make sure this accounts for multiple gameweeks..
    # this doesn't seem to nicely handle multiple gameweeks... might
    # need to use the player api endpoint instead... :/
    def parse_gameweek_stats(self):
        stats = []
        for player in self.response_data["elements"]:
            d_ = {"id": player["id"]}
            d_.update(player["stats"])

            # TODO: Not sure how this will work for multiple gameweeks
            d_.update({"fixture_id": player["explain"][0]["fixture"]})
            stats.append(d_)
        return stats


class PlayerScraper(FPLScraperBase):
    URL_BASE = "https://fantasy.premierleague.com/api/element-summary/{ID}/"

    def __init__(self, id):
        super().__init__()
        self.id = id
        self.url = self.URL_BASE.format(ID=id)

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, url):
        self._url = url

    def scrape(self):
        d = self.get_response(self.url)
        stats = d["history"]
        self.scraped_data[f"player_stats_{self.id}"] = stats
        return self.scraped_data


def run_scrapers(elements=[]):
    raw = "/Users/toby/Dev/lionel/data/raw/"
    today = dt.date.today().strftime("%Y%m%d")
    scrapers = [GenInfoScraper(), FixtureScraper()]
    scraped_data_ = [scraper.scrape() for scraper in scrapers]
    scraped_data = {}
    for d in scraped_data_:
        scraped_data.update(d)

    if not elements:
        elements = scraped_data["element_map"].keys()
    for el in elements:
        player = PlayerScraper(el)
        player.scrape()
        scraped_data.update(player.scraped_data)

    json.dump(scraped_data, open(raw + f"scraped_data_{today}.json", "w"))
    return scraped_data
