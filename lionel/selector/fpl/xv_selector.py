from collections.abc import Callable, Mapping, Sequence

from pydantic import TypeAdapter

from ..core.base_selector import Selector
from ..core.constraints import Constraint
from ..core.player import Player
from .constraints import FplConstraint, points, squad


class XVSelector(Selector):
    """
    Builds an FPL squad of 15: budget, position quotas, at most 3 players per team,
    exactly 1 captain whose points count twice. See `squad()` and `points()`.
    Extra `constraints` are applied after the standard ones.
    """

    CONSTRAINT_ADAPTER = TypeAdapter(FplConstraint)

    def __init__(
        self,
        players: Sequence[Player | Mapping],
        *,
        budget: float = 1000.0,
        constraints: Sequence[Constraint | Mapping | Callable] = (),
    ) -> None:
        super().__init__(
            players,
            objective=points(captain=True),
            constraints=[*squad(budget), *constraints],
            flags={"selected": "xv", "captain": "captain"},
        )
