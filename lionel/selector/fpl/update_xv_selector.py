from collections.abc import Callable, Mapping, Sequence

from ..core.constraints import Constraint
from ..core.player import Player
from .constraints import MaxTransfers
from .xv_selector import XVSelector


class UpdateXVSelector(XVSelector):
    """
    Updates an existing XV by making a limited number of transfers.

    Expects the candidates to have:
      - 'xv' flag with exactly 15 players set (the existing squad).
      - Possibly 'captain' if you want to track an existing captain, though
        reassigning captains may be decided by the solver.

    Adds one constraint, `MaxTransfers`: you can only add up to 'max_transfers'
    new players (i.e., those who previously had xv=0).
    """

    def __init__(
        self,
        players: Sequence[Player | Mapping],
        *,
        max_transfers: int = 1,
        budget: float = 1000.0,
        constraints: Sequence[Constraint | Mapping | Callable] = (),
    ) -> None:
        super().__init__(players, budget=budget, constraints=[MaxTransfers(n=max_transfers), *constraints])
        self.max_transfers = max_transfers

        # Validate that the existing team has exactly 15 players selected
        if sum(p.xv for p in self.players) != 15:
            raise ValueError("The existing squad must have exactly 15 players set to 'xv=1'.")
