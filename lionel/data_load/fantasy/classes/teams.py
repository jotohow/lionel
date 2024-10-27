import copy

import pandas as pd

from lionel.constants import RAW, TEAM_MAP
from lionel.data_load.fantasy.base_class import AbstractLoader


class TeamLoader(AbstractLoader):
    pass

    @property
    def key(self):
        return "team_map"

    def read_from_file(self, season):
        return super().read_from_file(RAW / f"team_ids_{season}.csv")

    def read_from_scrape(self):
        data = pd.DataFrame(self._read_from_scrape())
        data = data.replace({"name": TEAM_MAP["team_name"]})
        data["web_id"] = data["web_id"].astype(int)
        self.data = data[["web_id", "name"]]
        return self.data

    def _clean_teams(self):
        assert self.data, "Load the data first"
        # df = copy.copy(self.data)
        self.data = self.data[["id", "name"]].rename(columns={"id": "web_id"})
        return self.data

    def get_existing_teams(self):
        q = "SELECT id, name FROM teams"
        teams = pd.read_sql(q, self.db_manager.engine.raw_connection()).set_index("id")
        self.teams = {v["name"]: k for k, v in teams.to_dict(orient="index").items()}
        return self.teams
    
    def get_existing_team_seasons(self):
        pass

    def process(self):
        assert self.data, "Load the data first"
        df = self._clean_teams()
        _ = self.get_existing_teams()

        # Teans
        self.new_teams = df[~df["name"].isin(self.teams.keys().to_list())]

        # TODO: Team seasons 
        pass

    def run():
        pass
