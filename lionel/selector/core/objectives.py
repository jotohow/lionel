import pulp
from pydantic import BaseModel, ConfigDict

from .constraints import Variables
from .player import Player


class Objective(BaseModel):
    """
    Base class. `build` returns the expression that the selector maximises.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    type: str

    def build(self, players: list[Player], v: Variables) -> pulp.LpAffineExpression:
        raise NotImplementedError
