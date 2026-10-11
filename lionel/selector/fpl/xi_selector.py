from collections.abc import Callable, Mapping, Sequence

from pydantic import TypeAdapter

from ..core.base_selector import Selector
from ..core.constraints import Constraint
from ..core.player import Player
from .constraints import FplConstraint, lineup, points


class XISelector(Selector):
    """
    Selects a lineup of 11 from a candidate set (e.g., the 15 already in a squad):
    position ranges, maximising predicted points. See `lineup()` and `points()`.
    Extra `constraints` are applied after the standard ones.
    """

    CONSTRAINT_ADAPTER = TypeAdapter(FplConstraint)

    def __init__(
        self,
        players: Sequence[Player | Mapping],
        *,
        constraints: Sequence[Constraint | Mapping | Callable] = (),
    ) -> None:
        super().__init__(
            players,
            objective=points(captain=False),
            constraints=[*lineup(), *constraints],
            flags={"selected": "xi"},
        )
