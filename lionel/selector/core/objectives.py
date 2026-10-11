# TODO: Add common objectives that can be used by all selectors

from abc import ABC, abstractmethod

import pandas as pd
import pulp



class Objective(ABC):
    pass


class DefaultObjective(Objective):
    def __init__(self, candidate_df: pd.DataFrame, decision_vars: list[pulp.LpVariable], pred_var: str) -> None:
        self.candidate_df = candidate_df
        self.decision_vars = decision_vars
        self.pred_var = pred_var

    def __call__(self) -> pulp.LpAffineExpression:
        return pulp.lpSum(
            self.decision_vars[i] * self.candidate_df.iloc[i][self.pred_var] for i in range(self.num_players)
        )
