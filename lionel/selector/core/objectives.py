# TODO: Add common objectives that can be used by all selectors

from abc import ABC

import pulp

from .base_selector import DecisionVars
from .player import Player


class Objective(ABC):
    pass


class DefaultObjective(Objective):
    def __init__(self, players: list[Player], decision_vars: DecisionVars, pred_var: str = "predicted_points") -> None:
        self.players = players
        self.decision_vars = decision_vars
        self.pred_var = pred_var

    def __call__(self) -> pulp.LpAffineExpression:
        return pulp.lpSum(getattr(p, self.pred_var) * self.decision_vars[p.player_id] for p in self.players)
