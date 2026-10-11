"""The result of a selector run."""

from collections.abc import Mapping

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field

from .player import OPTIONAL_FIELDS, REQUIRED_FIELDS, Player, resolve_columns

OPTIMAL = "Optimal"


class InfeasibleSelectionError(Exception):
    """No selection satisfies all the constraints."""

    def __init__(self, status: str) -> None:
        super().__init__(f"Selector did not find an optimal selection (solver status: {status}).")
        self.status = status


class Selection(BaseModel):
    """
    The result of `select()`.

    `status` is the solver status ("Optimal", "Infeasible", ...). If it is not
    "Optimal", `selected` is empty and `objective` is None. Call
    `raise_for_status()` to raise on failure.

    `candidates` holds all the input players with the flags set. It is not
    part of `model_dump()` or the JSON, so that the output stays small.
    """

    model_config = ConfigDict(frozen=True)

    status: str
    objective: float | None = None
    selected: list[Player] = Field(default_factory=list)
    captain: Player | None = None
    candidates: list[Player] = Field(default_factory=list, exclude=True)

    def raise_for_status(self) -> None:
        if self.status != OPTIMAL:
            raise InfeasibleSelectionError(self.status)

    def to_dataframe(self, columns: Mapping[str, str] | None = None) -> pd.DataFrame:
        """
        All the candidates, one row each in input order. The flags are 0/1 ints.
        `columns` maps `Player` field names to column names. The `name` column
        is left out when no player has a name.
        """
        columns = resolve_columns(columns)
        fields = [*REQUIRED_FIELDS, *OPTIONAL_FIELDS]
        if all(p.name is None for p in self.candidates):
            fields.remove("name")
        rows = [
            {columns[f]: _cell(getattr(p, f)) for f in fields}
            for p in self.candidates
        ]
        return pd.DataFrame(rows, columns=[columns[f] for f in fields])


def _cell(value: object) -> object:
    """Flags are written as 0/1 ints, as the DataFrame contract expects."""
    return int(value) if isinstance(value, bool) else value
