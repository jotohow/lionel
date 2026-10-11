"""
Constraints are data: small, named, JSON-serialisable models.

Each constraint has one method, `apply(players, v)`, that returns PuLP constraints.
`v["selected"]` maps each player_id to its binary "selected" variable. Other variable
families, such as `v["captain"]`, are created on first use.
"""

from collections.abc import Callable
from typing import Annotated, Any, Literal, Union

import pulp
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .player import Player, PlayerId, coerce_player_id

DecisionVars = dict[PlayerId, pulp.LpVariable]
VARIABLE_PREFIX = {"selected": "x", "captain": "capt"}


class Variables(dict[str, DecisionVars]):
    """Family name -> player_id -> binary variable. A family is created when first used."""

    def __init__(self, players: list[Player]) -> None:
        super().__init__()
        self.players = players

    def __missing__(self, family: str) -> DecisionVars:
        prefix = VARIABLE_PREFIX.get(family, family)
        self[family] = {
            p.player_id: pulp.LpVariable(f"{prefix}_{i}", cat=pulp.LpBinary) for i, p in enumerate(self.players)
        }
        return self[family]


class Constraint(BaseModel):
    """
    Base class. `type` chooses the class when a constraint is given as a dict.
    `name` labels the constraint in the solver rows and in `Selection.constraints`.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    type: str
    name: str | None = Field(default=None, pattern=r"^\w+$")

    def label(self) -> str:
        return self.name or self.type

    def apply(self, players: list[Player], v: Variables) -> list[pulp.LpConstraint]:
        raise NotImplementedError


class Size(Constraint):
    """Exactly `n` players are selected."""

    type: Literal["size"] = "size"
    n: int

    def apply(self, players: list[Player], v: Variables) -> list[pulp.LpConstraint]:
        return [pulp.lpSum(v["selected"].values()) == self.n]


class _PlayerSet(Constraint):
    players: list[PlayerId] = Field(min_length=1)

    @field_validator("players", mode="before")
    @classmethod
    def _coerce_ids(cls, ids: object) -> object:
        return [coerce_player_id(i) for i in ids] if isinstance(ids, list) else ids

    @field_validator("players")
    @classmethod
    def _unique_ids(cls, ids: list) -> list:
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate player ids")
        return ids

    def _total(self, v: Variables) -> pulp.LpAffineExpression:
        selected = v["selected"]
        missing = [i for i in self.players if i not in selected]
        if missing:
            raise ValueError(f"unknown player ids {missing}")
        return pulp.lpSum(selected[i] for i in self.players)


class Include(_PlayerSet):
    """All of these players are selected."""

    type: Literal["include"] = "include"

    def apply(self, players: list[Player], v: Variables) -> list[pulp.LpConstraint]:
        return [self._total(v) >= len(self.players)]


class IncludeAny(_PlayerSet):
    """At least one of these players is selected."""

    type: Literal["include_any"] = "include_any"

    def apply(self, players: list[Player], v: Variables) -> list[pulp.LpConstraint]:
        return [self._total(v) >= 1]


class Exclude(_PlayerSet):
    """None of these players is selected."""

    type: Literal["exclude"] = "exclude"

    def apply(self, players: list[Player], v: Variables) -> list[pulp.LpConstraint]:
        return [self._total(v) <= 0]


class Custom(Constraint):
    """
    A Python callable `(players, x) -> LpConstraint | list[LpConstraint]`, where `x` maps
    player_id to the "selected" variable. For Python authors only: it is not JSON.
    """

    type: Literal["custom"] = "custom"
    fn: Callable[..., Any]

    def label(self) -> str:
        fn_name = getattr(self.fn, "__name__", "custom")
        return self.name or ("custom" if fn_name == "<lambda>" else fn_name)

    def apply(self, players: list[Player], v: Variables) -> list[pulp.LpConstraint]:
        result = self.fn(players, v["selected"])
        return result if isinstance(result, list) else [result]


# The constraints that can be sent as JSON, chosen by `type`.
CoreConstraint = Annotated[
    Union[Size, Include, IncludeAny, Exclude],
    Field(discriminator="type"),
]
