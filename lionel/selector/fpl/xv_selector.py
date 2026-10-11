from collections.abc import Mapping, Sequence

import pulp

from ..core.base_selector import BaseSelector, DecisionVars
from ..core.player import Player, PlayerId


class XVSelector(BaseSelector):
    """
    A concrete subclass for building an FPL squad subject to standard constraints:
      - Budget limit
      - Squad size = 15
      - Max 3 players per team
      - Position minimums (e.g. 2 GKs, 5 DEF, 5 MID, 3 FWD)
      - Exactly 1 captain (doubling predicted points)
      - Objective: maximize sum of (predicted_points) + an additional (predicted_points) for the captain
    """

    # Example distribution. Adjust as needed.
    POS_CONSTRAINTS: dict[str, int] = {"GK": 2, "DEF": 5, "MID": 5, "FWD": 3}
    MAX_PER_TEAM: int = 3

    def __init__(
        self,
        players: Sequence[Player | Mapping],
        *,
        budget: float = 1000.0,
    ) -> None:
        super().__init__(players)
        self.budget = budget

        # Create captain decision variables (one per player)
        self.captain_vars: DecisionVars = {
            p.player_id: pulp.LpVariable(f"capt_{i}", cat=pulp.LpBinary) for i, p in enumerate(self.players)
        }

        # Objective: sum( (x_i + c_i)*predicted_points )
        self.set_objective_function(self._objective_with_captains)

        # Add constraints
        self.add_constraint(self._constraint_xv_size)
        self.add_constraint(self._constraint_budget)
        self.add_constraint(self._constraint_positions)
        self.add_constraint(self._constraint_max_team)
        self.add_constraint(self._constraint_exactly_one_captain)
        self.add_constraint(self._constraint_captain_must_be_selected)

    def _objective_with_captains(self, players: list[Player], x: DecisionVars) -> pulp.LpAffineExpression:
        """
        Maximize sum of predicted points + an extra predicted_points for the captain.
        i.e. (x_i + c_i)*predicted_points[i].
        """
        return pulp.lpSum((x[p.player_id] + self.captain_vars[p.player_id]) * p.predicted_points for p in players)

    def _constraint_xv_size(self, players: list[Player], x: DecisionVars) -> pulp.LpConstraint:
        """Enforce exactly 15 selected players."""
        return pulp.lpSum(x.values()) == 15

    def _constraint_budget(self, players: list[Player], x: DecisionVars) -> pulp.LpConstraint:
        """Total cost of selected players must not exceed the budget."""
        return pulp.lpSum(x[p.player_id] * p.price for p in players) <= self.budget

    def _constraint_positions(self, players: list[Player], x: DecisionVars) -> list[pulp.LpConstraint]:
        """
        Enforce standard position requirements.
        Example: {'GK':2, 'DEF':5, 'MID':5, 'FWD':3}.
        """
        return [
            pulp.lpSum(x[p.player_id] for p in players if p.position == pos) == required_count
            for pos, required_count in self.POS_CONSTRAINTS.items()
        ]

    def _constraint_max_team(self, players: list[Player], x: DecisionVars) -> list[pulp.LpConstraint]:
        """
        Enforce no more than MAX_PER_TEAM players from the same club.
        """
        unique_teams = dict.fromkeys(p.team for p in players)
        return [
            pulp.lpSum(x[p.player_id] for p in players if p.team == team) <= self.MAX_PER_TEAM for team in unique_teams
        ]

    def _constraint_exactly_one_captain(self, players: list[Player], x: DecisionVars) -> pulp.LpConstraint:
        """Enforce exactly 1 captain among all selected players."""
        return pulp.lpSum(self.captain_vars.values()) == 1

    def _constraint_captain_must_be_selected(self, players: list[Player], x: DecisionVars) -> list[pulp.LpConstraint]:
        """
        For each player i: x_i >= c_i
        (i.e. a player can't be captain if not in the team).
        """
        return [x[p.player_id] - self.captain_vars[p.player_id] >= 0 for p in players]

    def _flags(self, selected_ids: set[PlayerId]) -> dict[str, set[PlayerId]]:
        """Sets 'xv' for the selected players and 'captain' for the captain."""
        return {"xv": selected_ids, "captain": self._chosen(self.captain_vars)}
