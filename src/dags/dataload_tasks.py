import datetime as dt
import json

import luigi
from scrapl.fpl.runner import run_scrapers

import lionel.data_load.fantasy.load_fixtures as load_fixtures
import lionel.data_load.fantasy.load_gameweeks as load_gameweeks
import lionel.data_load.fantasy.load_players as load_players
import lionel.data_load.fantasy.load_stats as load_stats
import lionel.data_load.fantasy.load_teams as load_teams
import lionel.data_load.fantasy.scrape as scrape
from lionel.constants import BASE, DATA, RAW, TODAY

from .utils import dbm, next_gw, season

today = TODAY.strftime("%Y%m%d")


class DataLoadTask(luigi.Task):
    """Base class for data loading tasks"""

    task_namespace = "Dataload"

    pass


class GameWeekEnded(luigi.ExternalTask):
    """Check if the most recent gameweek has ended"""

    task_namespace = "Dataload"
    # Ref example https://github.com/spotify/luigi/blob/829fc0c36ecb4d0ae4f0680dec6d538577b249a2/examples/top_artists.py#L28
    gameweek = luigi.IntParameter(default=next_gw - 1)  # TODO
    season = luigi.IntParameter(default=season)

    def complete(self):

        # Get the last kickoff time for the previous gameweek
        q = f"""
        SELECT kickoff_time 
        FROM fixtures
        WHERE gameweek = {self.gameweek} AND season = {self.season}
        ORDER BY kickoff_time DESC;
        """
        ds = dbm.query(q)[0][0]
        last_kickoff = dt.datetime.strptime(ds, "%Y-%m-%dT%H:%M:%SZ")

        # finished if we're the day after the last kickoff
        gw_ended = last_kickoff.date() < dt.date.today()
        return gw_ended


class Scrape(DataLoadTask):

    # next_gw, season = get_next_gw_season(dbm)

    def requires(self):
        return GameWeekEnded(next_gw - 1, season)
        # return GameWeekEnded()

    def output(self):
        p = str(RAW / f"scraped_data_{today}.json")
        return luigi.LocalTarget(p)

    def run(self):
        # delete previous scrapes?

        data = run_scrapers()
        with open(self.output().path, "w") as fp:
            json.dump(data, fp)


# NOTE: Problem that load code uses dates but now I want to use gw/season
class LoadGameweeks(DataLoadTask):

    def requires(self):
        return Scrape()

    def output(self):
        return luigi.LocalTarget(str(BASE / f"logs/gameweeks_loaded_{today}.txt"))

    def run(self):
        load_gameweeks.load_from_scrape(dbm, season)
        with open(self.output().path, "w") as f:
            f.write("Task completed successfully.")


class LoadTeams(DataLoadTask):
    def requires(self):
        return Scrape()

    def output(self):
        return luigi.LocalTarget(str(BASE / f"logs/teams_loaded_{today}.txt"))

    def run(self):
        load_teams.load_from_scrape(dbm, season)
        with open(self.output().path, "w") as f:
            f.write("Task completed successfully.")


class LoadFixtures(DataLoadTask):
    def requires(self):
        # require load teams and load gameweeks
        return [LoadTeams(), LoadGameweeks()]

    def output(self):
        return luigi.LocalTarget(str(BASE / f"logs/fixtures_loaded_{today}.txt"))

    def run(self):
        load_fixtures.load_from_scrape(dbm, season)
        with open(self.output().path, "w") as f:
            f.write("Task completed successfully.")


class LoadPlayers(DataLoadTask):
    def requires(self):
        return Scrape()

    def output(self):
        return luigi.LocalTarget(str(BASE / f"logs/players_loaded_{today}.txt"))

    def run(self):
        load_players.load_from_scrape(dbm, season)
        with open(self.output().path, "w") as f:
            f.write("Task completed successfully.")


class LoadStats(DataLoadTask):
    def requires(self):
        return [LoadFixtures(), LoadPlayers()]

    def output(self):
        return luigi.LocalTarget(str(BASE / f"logs/stats_loaded_{today}.txt"))

    def run(self):
        load_stats.load_from_scrape(dbm, season)
        with open(self.output().path, "w") as f:
            f.write("Task completed successfully.")


if __name__ == "__main__":
    luigi.build(
        [
            Scrape(),
            LoadGameweeks(),
            LoadTeams(),
            LoadFixtures(),
            LoadPlayers(),
            LoadStats(),
        ]
    )
