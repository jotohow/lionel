"""DAGS to train the model"""

import json

import luigi
import pandas as pd

from lionel.constants import BASE
from lionel.model.hierarchical import FPLPointsModel

from .utils import dbm, next_gw, season, today


# TODO: Add triggers for this. How often do I want to do it?
class TrainModel(luigi.Task):
    """Train the Lionel model"""

    season = luigi.IntParameter(default=25)
    next_gw = luigi.IntParameter(default=8)

    def output(self):
        path = str(BASE / f"models/hm_{today}.nc")
        return luigi.LocalTarget(path)

    def run(self):

        # initialise the model
        # extract the sampler and model configs
        try:
            with open(BASE / "src/dags/sampler_config.json") as f:
                sampler_config = json.load(f)
        except:
            sampler_config = None
        print("sampler:", sampler_config)

        try:
            with open(BASE / "src/dags/sampler_config.json") as f:
                model_config = json.load(f)
        except:
            model_config = None

        fplm = FPLPointsModel(sampler_config=sampler_config, model_config=model_config)

        # Extract the training data, fit and save the model
        q = f"""
        SELECT * FROM training 
        WHERE 
            (season = {self.season} AND gameweek < {self.next_gw}) 
            OR (season = {self.season-1} AND gameweek > {self.next_gw});
        """
        data = pd.read_sql(q, dbm.engine.raw_connection())
        fplm.fit(data, data.points)
        fplm.save(self.output().path)


if __name__ == "__main__":
    luigi.build([TrainModel(season=season, next_gw=next_gw)])
