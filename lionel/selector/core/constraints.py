# TODO: Add common constraints that can be used by all selectors

from abc import ABC, abstractmethod

import pulp


class Constraint(ABC):
    pass


class CountConstraint(Constraint):
    def __init__(self, candidate_df, decision_vars, required_count):
        self.candidate_df = candidate_df
        self.decision_vars = decision_vars
        self.required_count = required_count

    def less_than(self, max_count):
        return pulp.lpSum(self.decision_vars) <= max_count

    def greater_than(self, min_count):
        return pulp.lpSum(self.decision_vars) >= min_count

    def equal_to(self, required_count):
        return pulp.lpSum(self.decision_vars) == required_count


class SumConstraint(Constraint):
    def __init__(self, candidate_df, decision_vars, sum_var):
        self.candidate_df = candidate_df
        self.decision_vars = decision_vars
        self.sum_var = sum_var

    def less_than(self, max_sum):
        return pulp.lpSum(self.decision_vars * self.candidate_df[self.sum_var]) <= max_sum

    def greater_than(self, min_sum):
        return pulp.lpSum(self.decision_vars * self.candidate_df[self.sum_var]) >= min_sum
