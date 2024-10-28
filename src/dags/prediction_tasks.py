"""Tasks to make predictions with the model and store them in the database."""

from pathlib import Path

import luigi
import pandas as pd
import sqlalchemy as sa

import lionel.data_load.model.predict as predict
import lionel.selector as selector
from lionel.constants import ANALYSIS, BASE, TODAY
from lionel.model.hierarchical import FPLPointsModel

from .utils import dbm, next_gw, season


class ModelExists(luigi.ExternalTask):
    """Check if the model has been trained"""

    def output(self):
        # Return the most recently trained model
        p = BASE / "models/"
        models = p.glob("*.nc")
        models = sorted(models, key=lambda x: int(x.stem.split("_")[1]), reverse=True)
        return luigi.LocalTarget(str(models[0]))


class PredictionTask(luigi.Task):
    """Base class for prediction tasks"""

    task_namespace = "Prediction"

    pass


class PredictScorelines(PredictionTask):
    next_gw = luigi.IntParameter()
    season = luigi.IntParameter()

    def requires(self):
        return ModelExists()

    def output(self):
        p = str(ANALYSIS / f"pred_score_{TODAY.strftime('%Y%m%d')}.csv")
        return luigi.LocalTarget(p)

    def run(self):
        model = FPLPointsModel.load(self.input().path)
        data = predict.build_pred_data(dbm, self.next_gw, self.season)
        df_scoreline = predict.predict_scoreline(data, model)
        df_scoreline.to_csv(self.output().path, index=False)


# TODO: Does this need to only be once per model?
class PredictPlayers(PredictionTask):
    next_gw = luigi.IntParameter()
    season = luigi.IntParameter()

    def requires(self):
        return ModelExists()

    def output(self):
        p = str(ANALYSIS / f"pred_player_{TODAY.strftime('%Y%m%d')}.csv")
        return luigi.LocalTarget(p)

    def run(self):
        model = FPLPointsModel.load(self.input().path)
        data = predict.build_pred_data(dbm, self.next_gw, self.season)
        df_player = predict.predict_players(data, self.next_gw, model)
        df_player.to_csv(self.output().path, index=False)


class LoadScorelines(PredictionTask):
    next_gw = luigi.IntParameter()
    season = luigi.IntParameter()

    def requires(self):
        return PredictScorelines(next_gw=self.next_gw, season=self.season)

    def output(self):
        p = str(BASE / f'logs/scorelines_loaded_{TODAY.strftime("%Y%m%d")}.txt')
        return luigi.LocalTarget(p)

    def run(self):
        data = pd.read_csv(self.input().path)
        data.to_sql(
            "scorelines", dbm.engine.raw_connection(), if_exists="replace", index=False
        )
        with open(self.output().path, "w") as f:
            f.write("Task completed successfully.")


class SelectTeam(PredictionTask):
    next_gw = luigi.IntParameter()
    season = luigi.IntParameter()

    def requires(self):
        return PredictPlayers(next_gw=self.next_gw, season=self.season)

    def output(self):
        p = str(ANALYSIS / f'selection_{TODAY.strftime("%Y%m%d")}.csv')
        return luigi.LocalTarget(p)

    def run(self):
        preds = pd.read_csv(self.input().path)
        selections = selector.run_selection(preds)
        selections["gameweek"] = self.next_gw
        selections["season"] = self.season
        selections.to_csv(self.output().path, index=False)


class LoadSelection(PredictionTask):
    next_gw = luigi.IntParameter()
    season = luigi.IntParameter()

    def requires(self):
        return SelectTeam(next_gw=self.next_gw, season=self.season)

    def output(self):
        p = str(BASE / f'logs/selection_loaded_{TODAY.strftime("%Y%m%d")}.txt')
        return luigi.LocalTarget(p)

    def run(self):
        data = pd.read_csv(self.input().path)

        with dbm.engine.connect() as conn:
            conn.execute(
                sa.text(
                    "DELETE FROM selections WHERE gameweek = "
                    f"{self.next_gw} AND season = {self.season}"
                )
            )
        data.to_sql(
            "selections", dbm.engine.raw_connection(), if_exists="append", index=False
        )

        with open(self.output().path, "w") as f:
            f.write("Task completed successfully.")


class PlayerInference(PredictionTask):
    def requires(self):
        return ModelExists()

    def output(self):
        model_path = Path(str(self.input().path)).stem
        model_date = model_path.split("_")[1]
        return luigi.LocalTarget(str(ANALYSIS / f"player_inference_{model_date}.csv"))

    def run(self):
        model = FPLPointsModel.load(self.input().path)
        data = model.summarise_players()
        data.to_csv(self.output().path, index=False)


class LoadPlayerInference(PredictionTask):
    def requires(self):
        return PlayerInference()

    def output(self):
        p = str(BASE / f'logs/player_inference_loaded_{TODAY.strftime("%Y%m%d")}.txt')
        return luigi.LocalTarget(p)

    def run(self):
        data = pd.read_csv(self.input().path)

        with dbm.engine.connect() as conn:
            conn.execute(sa.text("DELETE FROM player_inference"))
        data.to_sql(
            "player_inference",
            dbm.engine.raw_connection(),
            if_exists="append",
            index=False,
        )

        with open(self.output().path, "w") as f:
            f.write("Task completed successfully.")


class TeamInference(PredictionTask):
    def requires(self):
        return ModelExists()

    def output(self):
        model_path = Path(str(self.input().path)).stem
        model_date = model_path.split("_")[1]
        return luigi.LocalTarget(str(ANALYSIS / f"team_inference_{model_date}.csv"))

    def run(self):
        model = FPLPointsModel.load(self.input().path)
        data = model.summarise_teams()
        data.to_csv(self.output().path, index=False)


class LoadTeamInference(PredictionTask):
    def requires(self):
        return TeamInference()

    def output(self):
        p = str(BASE / f'logs/team_inference_loaded_{TODAY.strftime("%Y%m%d")}.txt')
        return luigi.LocalTarget(p)

    def run(self):
        data = pd.read_csv(self.input().path)

        with dbm.engine.connect() as conn:
            conn.execute(sa.text("DELETE FROM team_inference"))
        data.to_sql(
            "team_inference",
            dbm.engine.raw_connection(),
            if_exists="append",
            index=False,
        )

        with open(self.output().path, "w") as f:
            f.write("Task completed successfully.")


if __name__ == "__main__":
    luigi.build(
        [
            PredictScorelines(next_gw=next_gw, season=season),
            PredictPlayers(next_gw=next_gw, season=season),
            LoadScorelines(next_gw=next_gw, season=season),
            LoadSelection(next_gw=next_gw, season=season),
            PlayerInference(),
            LoadPlayerInference(),
            TeamInference(),
            LoadTeamInference(),
        ]
    )
