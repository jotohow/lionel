# TODO: Add common constraints that can be used by all selectors

from abc import ABC, abstractmethod

import pulp


class Constraint(ABC):
    pass


class SumConstraint(Constraint):
    def __init__(self, candidate_df, decision_vars, sum_var=None):
        self.candidate_df = candidate_df
        self.decision_vars = decision_vars
        self.sum_var = sum_var

    def _total(self):
        if self.sum_var is None:
            return pulp.lpSum(self.decision_vars)
        return pulp.lpSum(self.decision_vars * self.candidate_df[self.sum_var])

    def less_than(self, max_sum):
        return self._total() <= max_sum

    def greater_than(self, min_sum):
        return self._total() >= min_sum

    def equal_to(self, required_sum):
        return self._total() == required_sum


class CountConstraint(SumConstraint):
    """A SumConstraint where every weight is 1."""

    def __init__(self, candidate_df, decision_vars):
        super().__init__(candidate_df, decision_vars, sum_var=None)
