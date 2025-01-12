"""
Base model for all models in the Lionel package.
"""

from abc import ABC, abstractmethod

import numpy as np
import pandas as pd


class LionelBaseModel(ABC):
    """
    An abstract base class for all Lionel prediction models.
    Provides a minimal interface that each model should implement.
    """

    @abstractmethod
    def fit(self, X: pd.DataFrame, y: np.ndarray, **kwargs):
        """
        Train the model on input features X and target y.
        """
        pass

    @abstractmethod
    def predict(self, X: pd.DataFrame, **kwargs) -> np.ndarray:
        """
        Generate point predictions for each row in X.
        """
        pass

    @abstractmethod
    def save(self, filepath: str):
        """
        Optional: save the model to the specified filepath.
        """
        pass

    @abstractmethod
    def load(self, filepath: str):
        """
        Optional: load the model from the specified filepath.
        """
        pass
