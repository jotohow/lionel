# TODO: Add common constraints that can be used by all selectors

from abc import ABC

import pulp

from .base_selector import DecisionVars
from .player import Player


class Constraint(ABC):
    pass


class SumConstraint(Constraint):
    """
    A constraint on the sum of `weight * x` over the selected players.
    `weight` names a numeric `Player` field. If it is None, every weight is 1.
    """

    def __init__(
        self,
        players: list[Player],
        decision_vars: DecisionVars,
        weight: str | None = None,
    ) -> None:
        self.players = players
        self.decision_vars = decision_vars
        self.weight = weight

    def _total(self) -> pulp.LpAffineExpression:
        if self.weight is None:
            return pulp.lpSum(self.decision_vars.values())
        return pulp.lpSum(getattr(p, self.weight) * self.decision_vars[p.player_id] for p in self.players)

    def less_than(self, max_sum: float) -> pulp.LpConstraint:
        return self._total() <= max_sum

    def greater_than(self, min_sum: float) -> pulp.LpConstraint:
        return self._total() >= min_sum

    def equal_to(self, required_sum: float) -> pulp.LpConstraint:
        return self._total() == required_sum


class CountConstraint(SumConstraint):
    """A SumConstraint where every weight is 1."""

    def __init__(
        self,
        players: list[Player],
        decision_vars: DecisionVars,
    ) -> None:
        super().__init__(players, decision_vars, weight=None)
