from pymc_experimental.model_builder import ModelBuilder
from typing import Dict, List, Optional, Tuple, Union
import pandas as pd
import numpy as np
import arviz as az
import pymc as pm
import pytensor.tensor as pt


class FPLPointsModel(ModelBuilder):
    __model_type__ = "FPLPointsModel"
    version = "0.1"

    def __init__(self, model_config: dict = None, sampler_config: dict = None):
        super().__init__(model_config, sampler_config)
        self.X_teams: Union[pd.DataFrame, np.ndarray] = None

    def build_model(self, X: pd.DataFrame, y: pd.Series, **kwargs):
        X_values = X
        y_values = y.values if isinstance(y, pd.Series) else y
        self._generate_and_preprocess_model_data(X_values, y_values)

        with pm.Model(coords=self.model_coords) as self.model:

            # Data that will be
            home_team = pm.Data("home_team", self.home_idx, dims="match")
            away_team = pm.Data("away_team", self.away_idx, dims="match")

            is_home = pm.Data("is_home", self.is_home, dims="player_app")
            player_app_idx_ = pm.Data(
                "player_app_idx_", self.player_app_idx, dims="player_app"
            )
            player_idx_ = pm.Data("player_idx_", self.player_idx, dims="player_app")

            # Extract priors from the model config
            beta_0_mu_prior = self.model_config.get("beta_intercept_mu_prior", 2)
            beta_0_sigma_prior = self.model_config.get("beta_intercept_sigma_prior", 2)
            beta_home_mu_prior = self.model_config.get("beta_home_mu_prior", 0.0)
            beta_home_sigma_prior = self.model_config.get("beta_home_sigma_prior", 1.0)
            sd_att_mu_prior = self.model_config.get("sd_att_mu_prior", 1)
            sd_def_mu_prior = self.model_config.get("sd_def_mu_prior", 1)
            mu_att_mu_prior = self.model_config.get("mu_att_mu_prior", 0)
            mu_att_sigma_prior = self.model_config.get("mu_att_sigma_prior", 1e-1)
            mu_def_mu_prior = self.model_config.get("mu_def_mu_prior", 0)
            mu_def_sigma_prior = self.model_config.get("mu_def_sigma_prior", 1e-1)
            v_alpha_prior = self.model_config.get("v_alpha_prior", 1)
            v_beta_prior = self.model_config.get("v_beta_prior", 20)

            # PRIORS SHOULD COME FROM THE MODEL CONFIG
            beta_0 = pm.Normal(
                "beta_intercept", mu=beta_0_mu_prior, sigma=beta_0_sigma_prior
            )
            beta_home = pm.Normal(
                "beta_home", mu=beta_home_mu_prior, sigma=beta_home_sigma_prior
            )
            sd_att = pm.HalfNormal("sd_att", sigma=sd_att_mu_prior)
            sd_def = pm.HalfNormal("sd_def", sigma=sd_def_mu_prior)
            mu_att = pm.Normal("mu_att", mu=mu_att_mu_prior, sigma=mu_att_sigma_prior)
            mu_def = pm.Normal("mu_def", mu=mu_def_mu_prior, sigma=mu_def_sigma_prior)

            atts = pm.Normal("atts", mu=mu_att, sigma=sd_att, dims="team")
            defs = pm.Normal("defs", mu=mu_def, sigma=sd_def, dims="team")

            u = pm.Dirichlet("prior_p", a=np.ones(3), dims="outcome")
            v = pm.Gamma(
                "dprior_v", alpha=v_alpha_prior, beta=v_beta_prior, dims="outcome"
            )

            # Why do we centre these?
            beta_attack = pm.Deterministic(
                "beta_attack", atts - pt.mean(atts), dims="team"
            )
            beta_defence = pm.Deterministic(
                "beta_defence", defs - pt.mean(defs), dims="team"
            )

            # could the indexing be wrong? the preds are just totally wrong...
            mu_home = pm.math.exp(
                beta_0 + beta_home + beta_attack[home_team] + beta_defence[away_team]
            )
            mu_away = pm.math.exp(
                beta_0 + beta_attack[away_team] + beta_defence[home_team]
            )

            # The issue is somewhere here
            home_goals = pm.Poisson(
                "home_goals",
                mu=mu_home,
                observed=self.home_goals,  # THIS IS THE only diff from above, but think it's fine
                dims="match",
            )
            away_goals = pm.Poisson(
                "away_goals", mu=mu_away, observed=self.away_goals, dims="match"
            )

            team_goals = pm.Deterministic(
                "team_goals",
                pm.math.switch(
                    is_home, home_goals[player_app_idx_], away_goals[player_app_idx_]
                ),
                dims="player_app",
            )
            team_goals_conceded = pm.Deterministic(
                "team_goals_conceded",
                pm.math.switch(
                    is_home, away_goals[player_app_idx_], home_goals[player_app_idx_]
                ),
                dims="player_app",
            )

            clean_sheet = pm.Deterministic(
                "clean_sheet",
                pm.math.switch(team_goals_conceded > 0, 0, 1),
                dims="player_app",
            )

            alpha = pm.Deterministic(
                "alpha",
                np.repeat((u * v)[np.newaxis, :], len(self.players), axis=0),
                dims=("player", "outcome"),
            )
            theta = pm.Dirichlet("theta", a=alpha, dims=("player", "outcome"))
            player_contribution_opportunities = pm.Multinomial(
                "player_contribution_opportunities",
                n=team_goals,
                p=theta[player_idx_],
                observed=self.X[
                    ["goals_scored", "assists", "no_contribution"]
                ].values,  # DEFO WRONG
                dims=("player_app", "outcome"),
            )
            player_re = pm.Normal("re_player", mu=0, sigma=2, dims="player")
            player_goals = player_contribution_opportunities[player_app_idx_, 0]
            player_assists = player_contribution_opportunities[player_app_idx_, 1]

            mu_points = pm.Deterministic(
                "mu_points",
                (
                    (5 * player_goals)
                    + (3 * player_assists)
                    + (1 * clean_sheet)
                    + player_re[player_idx_]
                ),
                dims="player_app",
            )
            points = pm.Normal(
                "points", mu=mu_points, observed=self.y, dims="player_app"
            )

    def _data_setter(
        self, X: Union[pd.DataFrame, np.ndarray], y: Union[pd.Series, np.ndarray] = None
    ):

        # TODO: Find the bug here that happens when we fit on new data

        final_match = self.match_idx.max() + 1
        X_teams_new = (
            X[["home_team", "away_team", "home_goals", "away_goals"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        match_idx_new = (
            pd.factorize(X_teams_new[["home_team", "away_team"]].apply(tuple, axis=1))[
                0
            ]
            + final_match
        )

        # using the string names in X['home_team'].values, map to the index in self.home_idx
        _ = X["home_team"].values
        home_teams = np.array([np.where(self.teams == team)[0][0] for team in _])
        _ = X["away_team"].values
        away_teams = np.array([np.where(self.teams == team)[0][0] for team in _])
        is_home = X["is_home"].values

        # Shouldn't be a problem that these don't rely on the old indices because
        # this function updates those old indices...
        players_new = X["player"].unique()
        player_app_idx_, _ = pd.factorize(
            X[["home_team", "away_team"]].apply(tuple, axis=1)
        )
        player_idx_ = [np.where(self.players == player)[0][0] for player in players_new]

        x_values = {
            "home_team": home_teams,
            "away_team": away_teams,
            "is_home": is_home,
            "player_app_idx_": player_app_idx_,
            "player_idx_": player_idx_,
        }
        new_coords = {
            "match": match_idx_new,
            "player_app": player_app_idx_,
            "player": players_new,
        }

        with self.model:
            pm.set_data(x_values, coords=new_coords)
            if y is not None:
                pm.set_data({"y_data": y.values if isinstance(y, pd.Series) else y})

    # The cols in df_2 should go in here
    def _generate_and_preprocess_model_data(
        self, X: Union[pd.DataFrame, pd.Series], y: Union[pd.Series, np.ndarray]
    ) -> None:
        """
        Process the data and generate the model coordinates.
        """

        COLS_NEEDED = [
            "player_name",
            "player_id",
            "home_team",
            "away_team",
            "home_goals",
            "away_goals",
            # 'position',  # NEED TO ADD THIS AS A PRIORTY RLY - because point scores are dependent on this
            "goals_scored",
            "assists",
            "no_contribution",
            "points",
            "is_home",
            "player",
        ]
        assert all(
            [col in X.columns for col in COLS_NEEDED]
        ), f"Missing columns: {set(COLS_NEEDED) - set(X.columns)}"

        X = X[COLS_NEEDED]

        # IDs
        ## Add meaningful name to unique ID
        X["player"] = X["player_id"].astype(str) + "_" + X["player_name"]
        player_idx, players = pd.factorize(X["player"])
        player_app_idx, player_apps = pd.factorize(
            X[["home_team", "away_team"]].apply(tuple, axis=1)
        )

        ## Team level
        X_teams = (
            X[["home_team", "away_team", "home_goals", "away_goals"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        home_idx, teams = pd.factorize(X_teams["home_team"], sort=True)
        away_idx, _ = pd.factorize(X_teams["away_team"], sort=True)
        match_idx, matches = pd.factorize(
            X_teams[["home_team", "away_team"]].apply(tuple, axis=1)
        )

        outcomes = ["goals_scored", "assists", "no_contribution"]

        # Add the various attributes
        self.X = X
        self.y = y

        self.X_teams = X_teams
        self.home_goals = self.X_teams["home_goals"].values
        self.away_goals = self.X_teams["away_goals"].values

        self.is_home = self.X["is_home"].values

        self.players = players
        self.player_idx = player_idx
        self.player_app_idx = player_app_idx
        self.teams = teams
        self.home_idx = home_idx
        self.away_idx = away_idx
        self.match_idx = match_idx

        self.model_coords = {
            "player": players,
            "player_app": player_app_idx,
            "team": teams,
            "match": match_idx,
            "outcome": outcomes,
        }

    @staticmethod
    def get_default_model_config() -> Dict:
        """
        Returns a class default config dict for model builder if no model_config is provided on class initialization.
        The model config dict is generally used to specify the prior values we want to build the model with.
        It supports more complex data structures like lists, dictionaries, etc.
        It will be passed to the class instance on initialization, in case the user doesn't provide any model_config of their own.
        """

        model_config: Dict = {
            "beta_intercept_mu_prior": 2,
            "beta_intercept_sigma_prior": 2,
            "beta_home_mu_prior": 0.0,
            "beta_home_sigma_prior": 1.0,
            "sd_att_mu_prior": 1,
            "sd_def_mu_prior": 1,
            "mu_att_mu_prior": 0,
            "mu_att_sigma_prior": 1e-1,
            "mu_def_mu_prior": 0,
            "mu_def_sigma_prior": 1e-1,
            "v_alpha_prior": 1,
            "v_beta_prior": 20,
        }
        return model_config

    @staticmethod
    def get_default_sampler_config() -> Dict:
        """
        Returns a class default sampler dict for model builder if no sampler_config is provided on class initialization.
        The sampler config dict is used to send parameters to the sampler .
        It will be used during fitting in case the user doesn't provide any sampler_config of their own.
        """
        sampler_config: Dict = {
            "draws": 250,
            "tune": 100,
            "chains": 3,
            "target_accept": 0.95,
            "progressbar": True,
        }
        return sampler_config

    @property
    def output_var(self):
        return "y"
