from collections.abc import Mapping, Sequence

import pulp

from ..core.base_selector import DecisionVars
from ..core.player import Player
from .xv_selector import XVSelector


class UpdateXVSelector(XVSelector):
    """
    Subclass that allows updating an existing XV by making a limited number of transfers.

    Expects the candidates to have:
      - 'xv' flag with exactly 15 players set (the existing squad).
      - Possibly 'captain' if you want to track an existing captain, though
        reassigning captains may be decided by the solver.

    Adds one extra constraint:
      - You can only add up to 'max_transfers' new players (i.e., those who previously had xv=0).
    """

    def __init__(
        self,
        players: Sequence[Player | Mapping],
        *,
        max_transfers: int = 1,
        budget: float = 1000.0,
    ) -> None:
        super().__init__(players, budget=budget)
        self.max_transfers = max_transfers

        # Validate that the existing team has exactly 15 players selected
        if sum(p.xv for p in self.players) != 15:
            raise ValueError("The existing squad must have exactly 15 players set to 'xv=1'.")

        # Add the constraint for the maximum number of new players
        self.add_constraint(self._constraint_max_transfers)

    def _constraint_max_transfers(self, players: list[Player], x: DecisionVars) -> pulp.LpConstraint:
        """
        For players who currently have xv=0 (not in the existing squad),
        limit how many of those can be added to at most 'max_transfers'.
        """
        return pulp.lpSum(x[p.player_id] for p in players if not p.xv) <= self.max_transfers
