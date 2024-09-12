from pymc_experimental.model_builder import ModelBuilder
from typing import Dict, List, Optional, Tuple, Union
import pandas as pd
import numpy as np
import arviz as az
import pymc as pm
import pytensor.tensor as pt
import xarray as xr


class FPLPointsModel(ModelBuilder):
    """
    A hierarchical model for predicting Fantasy Premier League (FPL) points.

    This model is built using the `pymc_experimental` library and extends the `ModelBuilder` class.

    Attributes:
        __model_type__ (str): The type of the model.
        version (str): The version of the model.
    """

    __model_type__ = "FPLPointsModel"
    version = "0.1"

    def build_model(self, X: pd.DataFrame, y: pd.Series, **kwargs):
        """
        Build the FPL points prediction model.

        Args:
            X (pd.DataFrame): The input features.
            y (pd.Series): The target variable.

        Returns:
            None
        """
        X_values = X
        y_values = y.values if isinstance(y, pd.Series) else y
        self._generate_and_preprocess_model_data(X_values, y_values)

        with pm.Model(coords=self.model_coords) as self.model:

            # Data
            positions = pm.Data("positions", self.position_idx, dims="player_app")
            home_team = pm.Data("home_team", self.home_idx, dims="match")
            away_team = pm.Data("away_team", self.away_idx, dims="match")
            is_home = pm.Data("is_home", self.is_home, dims="player_app")
            player_idx_ = pm.Data("player_idx_", self.player_idx, dims="player_app")
            player_app_idx_ = pm.Data(
                "player_app_idx_", self.player_app_idx, dims="player_app"
            )

            # Variable points for contributions by position
            goal_points = pm.Data(
                "goal_points", np.array([10, 6, 5, 4]), mutable=False, dims="position"
            )
            assist_points = 3
            clean_sheet_points = pm.Data(
                "clean_sheet_points",
                np.array([4, 4, 1, 0]),
                mutable=False,
                dims="position",
            )

            # Priors from model config
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

            # Team level model parameters
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

            beta_attack = pm.Deterministic(
                "beta_attack", atts - pt.mean(atts), dims="team"
            )
            beta_defence = pm.Deterministic(
                "beta_defence", defs - pt.mean(defs), dims="team"
            )

            mu_home = pm.math.exp(
                beta_0 + beta_home + beta_attack[home_team] + beta_defence[away_team]
            )
            mu_away = pm.math.exp(
                beta_0 + beta_attack[away_team] + beta_defence[home_team]
            )

            home_goals = pm.Poisson(
                "home_goals",
                mu=mu_home,
                observed=self.home_goals,
                dims="match",
            )
            away_goals = pm.Poisson(
                "away_goals", mu=mu_away, observed=self.away_goals, dims="match"
            )

            # Player level model parameters
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

            u = pm.Dirichlet("prior_p", a=np.ones(3), dims="outcome")
            v = pm.Gamma(
                "dprior_v", alpha=v_alpha_prior, beta=v_beta_prior, dims="outcome"
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
                observed=self.X[["goals_scored", "assists", "no_contribution"]].values,
                dims=("player_app", "outcome"),
            )
            player_goals = player_contribution_opportunities[player_app_idx_, 0]
            player_assists = player_contribution_opportunities[player_app_idx_, 1]

            # Random effect to account for yellow cards, bonus points, etc.
            player_re = pm.Normal("re_player", mu=0, sigma=2, dims="player")

            # Points calculation
            mu_points = pm.Deterministic(
                "mu_points",
                (
                    goal_points[positions] * player_goals
                    + assist_points * player_assists
                    + clean_sheet_points[positions] * clean_sheet
                    + player_re[player_idx_]
                ),
                dims="player_app",
            )

            points_pred = pm.Normal(
                "points_pred", mu=mu_points, observed=self.y, dims="player_app"
            )

    def _data_setter(
        self, X: Union[pd.DataFrame, np.ndarray], y: Union[pd.Series, np.ndarray] = None
    ):
        """
        Set the data for the model.

        Args:
            X (Union[pd.DataFrame, np.ndarray]): The input features.
            y (Union[pd.Series, np.ndarray], optional): The target variable.

        Returns:
            None
        """
        final_match = self.match_idx.max() + 1
        X_teams_new = (
            X[["home_team", "away_team", "home_goals", "away_goals"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        match_idx_new, _ = pd.factorize(
            X_teams_new[["home_team", "away_team"]].apply(tuple, axis=1)
        )
        match_idx_new += final_match

        _ = X_teams_new["home_team"].values  # changed from X to X_teams_new
        home_teams = np.array([np.where(self.teams == team)[0][0] for team in _])
        _ = X_teams_new["away_team"].values  # changed from X to X_teams_new
        away_teams = np.array([np.where(self.teams == team)[0][0] for team in _])
        is_home = X["is_home"].values

        player_app_idx_, _ = pd.factorize(
            X[["home_team", "away_team"]].apply(tuple, axis=1)
        )
        player_idx_ = [np.where(self.players == player)[0][0] for player in X["player"]]
        position_idx = np.array(X["position"].map(self.pos_map))

        x_values = {
            "home_team": home_teams,
            "away_team": away_teams,
            "is_home": is_home,
            "player_app_idx_": player_app_idx_,
            "player_idx_": player_idx_,
            "positions": position_idx,
        }
        new_coords = {
            "match": match_idx_new,
            "player_app": player_app_idx_,
            "player": X["player"].unique(),
        }

        with self.model:
            pm.set_data(x_values, coords=new_coords)
            if y is not None:
                pm.set_data({"y_data": y.values if isinstance(y, pd.Series) else y})

    def _generate_and_preprocess_model_data(
        self, X: Union[pd.DataFrame, pd.Series], y: Union[pd.Series, np.ndarray]
    ) -> None:
        """
        Process the data and generate the model coordinates.

        Args:
            X (Union[pd.DataFrame, pd.Series]): The input features.
            y (Union[pd.Series, np.ndarray]): The target variable.

        Returns:
            None
        """
        COLS_NEEDED = [
            "player_name",
            "player_id",
            "home_team",
            "away_team",
            "home_goals",
            "away_goals",
            "position",  # NEED TO ADD THIS AS A PRIORTY RLY - because point scores are dependent on this
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
        position_idx = np.array(X["position"].map(self.pos_map))

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
        self.position_idx = position_idx
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
            "position": ["GK", "DEF", "MID", "FWD"],
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
        return "points_pred"

    # Mask method from base class - it didn't work when kwarg predictions=True was passed
    def sample_posterior_predictive(self, X_pred, extend_idata, combined, **kwargs):
        """
        Sample from the model's posterior predictive distribution.

        Parameters
        ----------
        X_pred : array, shape (n_pred, n_features)
            The input data used for prediction using prior distribution..
        extend_idata : Boolean determining whether the predictions should be added to inference data object.
            Defaults to False.
        combined: Combine chain and draw dims into sample. Won't work if a dim named sample already exists.
            Defaults to True.
        **kwargs: Additional arguments to pass to pymc.sample_posterior_predictive

        Returns
        -------
        posterior_predictive_samples : DataArray, shape (n_pred, samples)
            Posterior predictive samples for each input X_pred
        """
        self._data_setter(X_pred)

        with self.model:  # sample with new input data
            post_pred = pm.sample_posterior_predictive(self.idata, **kwargs)
            if extend_idata:
                self.idata.extend(post_pred, join="right")
        group = (
            "predictions"
            if kwargs.get("predictions", False)
            else "posterior_predictive"
        )
        posterior_predictive_samples = az.extract(post_pred, group, combined=combined)

        return posterior_predictive_samples

    def predict_posterior(
        self,
        X_pred: np.ndarray | pd.DataFrame | pd.Series,
        extend_idata: bool = True,
        combined: bool = True,
        **kwargs,
    ) -> xr.DataArray:
        """
        Generate posterior predictive samples on unseen data.

        Parameters
        ----------
        X_pred : array-like if sklearn is available, otherwise array, shape (n_pred, n_features)
            The input data used for prediction.
        extend_idata : Boolean determining whether the predictions should be added to inference data object.
            Defaults to True.
        combined: Combine chain and draw dims into sample. Won't work if a dim named sample already exists.
            Defaults to True.
        **kwargs: Additional arguments to pass to pymc.sample_posterior_predictive

        Returns
        -------
        y_pred : DataArray, shape (n_pred, chains * draws) if combined is True, otherwise (chains, draws, n_pred)
            Posterior predictive samples for each input X_pred
        """

        # X_pred = self._validate_data(X_pred) # dropped to allow strings in X_pred
        posterior_predictive_samples = self.sample_posterior_predictive(
            X_pred, extend_idata, combined, **kwargs
        )

        if self.output_var not in posterior_predictive_samples:
            raise KeyError(
                f"Output variable {self.output_var} not found in posterior predictive samples."
            )

        return posterior_predictive_samples[self.output_var]

    @property
    def pos_map(self):
        positions = ["GK", "DEF", "MID", "FWD"]
        return {pos: i for i, pos in enumerate(positions)}
