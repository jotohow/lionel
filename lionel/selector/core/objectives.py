# TODO: Add common objectives that can be used by all selectors

from abc import ABC, abstractmethod

import pulp


class Objective(ABC):
    pass


class DefaultObjective(Objective):
    def __init__(self, candidate_df, decision_vars, pred_var):
        self.candidate_df = candidate_df
        self.decision_vars = decision_vars
        self.pred_var = pred_var

    def __call__(self):
        return pulp.lpSum(
            self.decision_vars[i] * self.candidate_df.iloc[i][self.pred_var] for i in range(self.num_players)
        )
