from pymc_experimental.model_builder import ModelBuilder
from typing import Dict, List, Optional, Tuple, Union, Any
import pandas as pd
import numpy as np
import arviz as az
import pymc as pm
import pytensor.tensor as pt
import xarray as xr
from pathlib import Path
from pymc.util import RandomState


class FPLPointsModel(ModelBuilder):
    """
    A hierarchical model for predicting Fantasy Premier League (FPL) points.

    This model is built using the `pymc_experimental` library and extends the `ModelBuilder` class.

    Attributes:
        __model_type__ (str): The type of the model.
        version (str): The version of the model.
    """

    _model_type_ = "FPLPointsModel"
    version = "0.1"

    EXPECTED_COLUMNS = [
        "player_name",
        "player_id",
        "gameweek",
        "season",
        "home_team",
        "away_team",
        "home_goals",
        "away_goals",
        "position",
        "minutes",
        "goals_scored",
        "assists",
        "no_contribution",
        "points",
        "is_home",
        "player",
    ]

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
            minutes = pm.Data("minutes", self.minutes, dims="player_app")

            # Account for different points for contributions by position
            goal_points = pm.Data(  # e.g. gk gets 10 points for a goal, fwd gets 4
                "goal_points", np.array([10, 6, 5, 4]), dims="position"
            )
            clean_sheet_points = pm.Data(  # gk/def gets 4 points, and so on
                "clean_sheet_points",
                np.array([4, 4, 1, 0]),
                dims="position",
            )
            assist_points = 3  # all positions get 3 points for an assist

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

            # Hyper-priors for player contribution probabilities
            score_alpha_prior = self.model_config.get("score_alpha_prior", 1)
            score_beta_prior = self.model_config.get("score_beta_prior", 0.5)
            assist_alpha_prior = self.model_config.get("assist_alpha_prior", 1)
            assist_beta_prior = self.model_config.get("assist_beta_prior", 0.5)
            neither_alpha_prior = self.model_config.get("neither_alpha_prior", 4)
            neither_beta_prior = self.model_config.get("neither_beta_prior", 3)
            alpha_score = pm.Gamma(
                "alpha_score",
                alpha=score_alpha_prior,
                beta=score_beta_prior,
                dims="position",
            )
            alpha_assist = pm.Gamma(
                "alpha_assist",
                alpha=assist_alpha_prior,
                beta=assist_beta_prior,
                dims="position",
            )
            alpha_neither = pm.Gamma(  # most likely
                "alpha_neither",
                alpha=neither_alpha_prior,
                beta=neither_beta_prior,
                dims="position",
            )

            theta = pm.Dirichlet(
                "theta",
                a=pm.math.stack([alpha_score, alpha_assist, alpha_neither], axis=-1),
                dims=("player", "position", "outcome"),
            )

            # Scale probabilities by minutes played
            _ = theta[player_idx_, positions, :]
            p_score = _[:, 0] * (minutes / 90)
            p_assist = _[:, 1] * (minutes / 90)
            p_neither = _[:, 2] * (minutes / 90) + (90 - minutes) / 90
            theta_scaled = pm.math.stack([p_score, p_assist, p_neither], axis=-1)

            # Player contribution opportunities conditional on team goals
            pco = pm.Multinomial(
                "player_contribution_opportunities",
                n=team_goals,
                p=theta_scaled,
                observed=self.X[["goals_scored", "assists", "no_contribution"]].values,
                dims=("player_app", "outcome"),
            )
            # should this just be * minutes? no because n is number of goals
            # in a game, so it should be scaled by minutes played in that game
            player_goals = pco[player_app_idx_, 0] * minutes / 90
            player_assists = pco[player_app_idx_, 1] * minutes / 90

            # Random effect to account for yellow cards, bonus points, etc.
            player_re_mu_prior = pm.Normal("player_re_mu_prior", sigma=2)
            player_re_sigma_prior = pm.HalfNormal("player_re_sigma_prior", sigma=2)
            player_re = pm.Normal(
                "re_player",
                mu=player_re_mu_prior,
                sigma=player_re_sigma_prior,
                dims="player",
            )

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

            # Noted that using played level sd for points prediction gave unworkable
            # results - chains didn't converge within a reasonable number of iterations
            points_pred = pm.Normal(
                "points_pred",
                mu=mu_points,
                sigma=1,
                observed=self.y,
                dims="player_app",
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

        # Team/match level indices
        final_match = self.match_idx.max() + 1
        X_teams_new = (
            X[["home_team", "away_team", "home_goals", "away_goals", "season"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        match_idx_new, _ = pd.factorize(
            X_teams_new[["home_team", "away_team", "season"]].apply(tuple, axis=1)
        )
        match_idx_new += final_match
        _ = X_teams_new["home_team"].values
        home_teams = np.array([np.where(self.teams == team)[0][0] for team in _])
        _ = X_teams_new["away_team"].values
        away_teams = np.array([np.where(self.teams == team)[0][0] for team in _])

        # Player level indices
        is_home = X["is_home"].values
        player_app_idx_, _ = pd.factorize(
            X[["home_team", "away_team", "season"]].apply(tuple, axis=1)
        )
        player_idx_ = [np.where(self.players == player)[0][0] for player in X["player"]]
        position_idx = np.array(X["position"].map(self.pos_map))

        # TODO: Allow some players to be estimated and others not
        if X["minutes"].isnull().sum() > 0:
            minutes_estimate = self.minutes_estimate[player_idx_]
        else:
            minutes_estimate = np.int32(X["minutes"].values)

        self.X_pred = X

        x_values = {  # new data to be passed into the model
            "home_team": home_teams,
            "away_team": away_teams,
            "is_home": is_home,
            "player_app_idx_": player_app_idx_,
            "player_idx_": player_idx_,
            "positions": position_idx,
            "minutes": minutes_estimate,
        }
        new_coords = {  # new dimensions to be added to the model
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
        # Filter columns in the input data
        assert all(
            [col in X.columns for col in self.EXPECTED_COLUMNS]
        ), f"Missing columns: {set(self.EXPECTED_COLUMNS) - set(X.columns)}"
        X = X[self.EXPECTED_COLUMNS]
        assert (
            X.player_id.nunique() == X.player.nunique()
        ), "Player names differ across the same ID - check for name changes"

        ## TODO: Log problematic ones
        # df_check = X[['player_id', 'player']].drop_duplicates()
        # df_check['n'] = df_check.groupby('player_id').transform('count')
        # df_check[df_check.n>1].sort_values('player_id')

        # Create ID variables for coords + attributes
        X["player"] = X["player_id"].astype(str) + "_" + X["player_name"]
        player_idx, players = pd.factorize(X["player"])
        player_app_idx, _ = pd.factorize(
            X[["home_team", "away_team", "season"]].apply(tuple, axis=1)
        )
        position_idx = np.array(X["position"].map(self.pos_map))
        minutes = X["minutes"].values
        minutes_estimate = self.get_minutes_estimate(X, players)

        X_teams = (
            X[["home_team", "away_team", "home_goals", "away_goals", "season"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )

        home_idx, teams = pd.factorize(X_teams["home_team"], sort=True)
        away_idx, _ = pd.factorize(X_teams["away_team"], sort=True)
        match_idx, matches = pd.factorize(
            X_teams[["home_team", "away_team", "season"]].apply(tuple, axis=1)
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
        self.minutes = minutes
        self.minutes_estimate = minutes_estimate
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
            "score_alpha_prior": 1,
            "score_beta_prior": 0.5,
            "assist_alpha_prior": 1,
            "assist_beta_prior": 0.5,
            "neither_alpha_prior": 4,
            "neither_beta_prior": 3,
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

        # NB: PyMC Marketing already implements this - maybe switch to use that
        # for stability and to avoid code duplication
        group = (
            "predictions"
            if kwargs.get("predictions", False)
            else "posterior_predictive"
        )

        return az.extract(post_pred, group, combined=combined)

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

        return posterior_predictive_samples[
            [self.output_var, "home_goals", "away_goals"]
        ]

    @classmethod
    def get_minutes_estimate(cls, df, players):
        assert df.player_id.nunique() == len(players)
        df_mins = (
            df.sort_values(["season", "gameweek"], ascending=[True, True])
            .groupby("player_id")
            .tail(3)
        )
        mins = df_mins.groupby(["player"])["minutes"].mean().reindex(players).values
        return np.int32(mins)

    @property
    def pos_map(self):
        positions = ["GK", "DEF", "MID", "FWD"]
        return {pos: i for i, pos in enumerate(positions)}

    def save(self, fname: str) -> None:
        """
        Save the model's inference data to a file.

        Parameters
        ----------
        fname : str
            The name and path of the file to save the inference data with model parameters.

        Returns
        -------
        None

        Raises
        ------
        RuntimeError
            If the model hasn't been fit yet (no inference data available).

        Examples
        --------

        """
        if not (self.idata is not None and "posterior" in self.idata):
            raise RuntimeError("The model hasn't been fit yet, call .fit() first")

        self.idata = self.set_idata_attrs()
        file = Path(str(fname))
        self.idata.to_netcdf(str(file))

    @property
    def _serializable_model_config(self) -> Dict[str, Union[int, float, Dict]]:
        """
        _serializable_model_config is a property that returns a dictionary with all the model parameters that we want to save.
        as some of the data structures are not json serializable, we need to convert them to json serializable objects.
        Some models will need them, others can just define them to return the model_config.
        """
        return self.model_config

    # Mask base class - ensure that y is passed.
    def fit(
        self,
        X: pd.DataFrame,
        y: pd.Series | np.ndarray,
        progressbar: bool = True,
        predictor_names: list[str] | None = None,
        random_seed: RandomState | None = None,
        **kwargs: Any,
    ) -> az.InferenceData:
        super().fit(X, y, progressbar, predictor_names, random_seed, **kwargs)


class VisBuilder:

    def __init__(self, model: FPLPointsModel):
        self.model = model

    def plot_team_strengths(self, idata, team_names, n_teams=10):
        pass

    def get_scoreline_predictions(self, idata, team_names, n_teams=10):
        pass

    def plot_player_strengths(self, idata, player_names):
        pass

    def get_top_players(self):
        pass
