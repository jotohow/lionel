from pathlib import Path
from typing import Any, Dict, Union

import arviz as az
import numpy as np
import pandas as pd
from pymc.util import RandomState
from pymc_marketing.model_builder import ModelBuilder

from lionel.model.base_model import LionelBaseModel


class BaseBayesianModel(ModelBuilder, LionelBaseModel):
    """
    Generic Bayesian base class that extends ModelBuilder from pymc_marketing.
    """

    def fit(
        self,
        X: pd.DataFrame,
        y: np.ndarray,
        progressbar: bool = True,
        predictor_names: list[str] | None = None,
        random_seed: RandomState | None = None,
        **kwargs: Any,
    ) -> az.InferenceData:
        """
        Mask base class - ensure that y is passed.
        Calls super().fit(...) to leverage pymc_marketing's ModelBuilder logic.
        """
        super().fit(X, y, progressbar, predictor_names, random_seed, **kwargs)

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
