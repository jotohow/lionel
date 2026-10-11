from collections.abc import Mapping, Sequence

import pulp

from ..core.base_selector import BaseSelector, DecisionVars
from ..core.player import Player, PlayerId


class XISelector(BaseSelector):
    """
    Class for selecting a team of 11 players from a candidate set (e.g., the 15 already in a squad).

    This class uses BaseSelector's methods to:
      - Maximize the predicted points.
      - Enforce exactly 11 selections.
      - Enforce position constraints (using POS_CONSTRAINTS).
    """

    # Minimum and maximum number of each position allowed
    POS_CONSTRAINTS: dict[str, tuple[int, int]] = {
        "GK": (1, 1),  # exactly 1 GK
        "DEF": (3, 5),
        "MID": (2, 5),
        "FWD": (1, 3),
    }

    def __init__(
        self,
        players: Sequence[Player | Mapping],
    ) -> None:
        super().__init__(players)

        self.set_objective_function(self.default_objective)
        self.add_constraint(self.constraint_xi_size)
        self.add_constraint(self.constraint_positions)

    def default_objective(self, players: list[Player], x: DecisionVars) -> pulp.LpAffineExpression:
        """
        By default, maximize the sum of predicted points for selected players.
        """
        return pulp.lpSum(x[p.player_id] * p.predicted_points for p in players)

    def constraint_xi_size(self, players: list[Player], x: DecisionVars) -> pulp.LpConstraint:
        """
        Enforces exactly 11 selected players.
        """
        return pulp.lpSum(x.values()) == 11

    def constraint_positions(self, players: list[Player], x: DecisionVars) -> list[pulp.LpConstraint]:
        """
        Enforces position constraints:
          - Each position must be between its defined min and max.
        """
        constraints = []
        for pos, (min_pos, max_pos) in self.POS_CONSTRAINTS.items():
            total = pulp.lpSum(x[p.player_id] for p in players if p.position == pos)
            constraints.append(total >= min_pos)
            constraints.append(total <= max_pos)
        return constraints

    def _flags(self, selected_ids: set[PlayerId]) -> dict[str, set[PlayerId]]:
        """Sets 'xi' for the selected players."""
        return {"xi": selected_ids}
