"""
Example: Extending BaseBayesianModel for a custom points model.

This script shows how a user can extend the BaseBayesianModel to
implement a custom Bayesian model, including the required methods.
"""

import numpy as np
import pandas as pd
import pymc as pm

from lionel.model.bayesian.base_bayesian_model import BaseBayesianModel


class MyCustomPointsModel(BaseBayesianModel):
    """
    A simplified Poisson model for 'points'.
    Inherits from BaseBayesianModel to reuse fit, save, and predict logic.

    Attributes:
        model: The PyMC model.
        idata: The inference data after fitting.
    """

    def build_model(self, X: pd.DataFrame, y: np.ndarray, **kwargs):
        """
        Build a minimal Poisson model for demonstration.
        """
        self.X = X
        self.y = y

        with pm.Model() as self.model:
            # One global rate parameter (lambda)
            lambda_ = pm.HalfNormal("lambda_", sigma=2.0)
            points = pm.Poisson("points", mu=lambda_, observed=self.y)

    def _data_setter(self, X: pd.DataFrame, y: np.ndarray = None):
        """
        Dummy implementation for required abstract method.
        """
        self.X_pred = X
        if y is not None:
            self.y = y

    def _generate_and_preprocess_model_data(
        self, X: pd.DataFrame, y: np.ndarray
    ) -> None:
        """
        Minimal implementation to satisfy the base class requirements.
        """
        self.X = X
        self.y = y

    @property
    def default_sampler_config(self):
        """
        Return default sampler configuration for fitting.
        """
        return {
            "draws": 500,
            "tune": 100,
            "chains": 2,
            "progressbar": True,
        }

    @property
    def output_var(self):
        """
        The output variable name for posterior predictions.
        """
        return "points"

    @property
    def default_model_config(self):
        """
        Return a dummy model configuration for this example.
        """
        return {}


# Example usage
def main():
    # A tiny dataset with 5 players and dummy feature
    df = pd.DataFrame(
        {
            "player": [f"player_{i}" for i in range(5)],
            "dummy_feature": np.random.randn(5),
        }
    )
    points = np.array([0, 2, 1, 3, 5])  # Dummy target variable

    # Initialize and fit the custom model
    model = MyCustomPointsModel()
    model.fit(df, points)

    # Predict using the same dataset for simplicity
    predictions = model.predict(df, extend_idata=False, predictions=True)
    df["predicted_points"] = predictions
    print(df)


if __name__ == "__main__":
    main()
