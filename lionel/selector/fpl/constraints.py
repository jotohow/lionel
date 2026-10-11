"""FPL constraints and objective, and the presets that the selectors use."""

from typing import Annotated, Literal, Union

import pulp
from pydantic import Field

from ..core.constraints import Constraint, Exclude, Include, IncludeAny, Size, Variables
from ..core.objectives import Objective
from ..core.player import Player, Position

MAX_PER_TEAM = 3
XV_QUOTAS: dict[Position, int] = {"GK": 2, "DEF": 5, "MID": 5, "FWD": 3}
XI_RANGES: dict[Position, tuple[int, int]] = {"GK": (1, 1), "DEF": (3, 5), "MID": (2, 5), "FWD": (1, 3)}


class Budget(Constraint):
    """The total price of the selected players is at most `max`."""

    type: Literal["budget"] = "budget"
    max: float

    def apply(self, players: list[Player], v: Variables) -> list[pulp.LpConstraint]:
        x = v["selected"]
        return [pulp.lpSum(x[p.player_id] * p.price for p in players) <= self.max]


class PositionQuota(Constraint):
    """Exactly `quotas[position]` selected players in each position."""

    type: Literal["position_quota"] = "position_quota"
    quotas: dict[Position, int]

    def apply(self, players: list[Player], v: Variables) -> list[pulp.LpConstraint]:
        x = v["selected"]
        return [
            pulp.lpSum(x[p.player_id] for p in players if p.position == pos) == n for pos, n in self.quotas.items()
        ]


class PositionRange(Constraint):
    """Between min and max selected players in each position."""

    type: Literal["position_range"] = "position_range"
    ranges: dict[Position, tuple[int, int]]

    def apply(self, players: list[Player], v: Variables) -> list[pulp.LpConstraint]:
        x = v["selected"]
        rows = []
        for pos, (lo, hi) in self.ranges.items():
            total = pulp.lpSum(x[p.player_id] for p in players if p.position == pos)
            rows += [total >= lo, total <= hi]
        return rows


class MaxPerTeam(Constraint):
    """At most `max` selected players from each team."""

    type: Literal["max_per_team"] = "max_per_team"
    max: int

    def apply(self, players: list[Player], v: Variables) -> list[pulp.LpConstraint]:
        x = v["selected"]
        teams = dict.fromkeys(p.team for p in players)
        return [pulp.lpSum(x[p.player_id] for p in players if p.team == team) <= self.max for team in teams]


class MaxTransfers(Constraint):
    """At most `n` selected players who are not in the current squad (`xv=False`)."""

    type: Literal["max_transfers"] = "max_transfers"
    n: int

    def apply(self, players: list[Player], v: Variables) -> list[pulp.LpConstraint]:
        x = v["selected"]
        return [pulp.lpSum(x[p.player_id] for p in players if not p.xv) <= self.n]


class OneCaptain(Constraint):
    """Exactly one captain."""

    type: Literal["one_captain"] = "one_captain"

    def apply(self, players: list[Player], v: Variables) -> list[pulp.LpConstraint]:
        return [pulp.lpSum(v["captain"].values()) == 1]


class CaptainInSquad(Constraint):
    """The captain is a selected player."""

    type: Literal["captain_in_squad"] = "captain_in_squad"

    def apply(self, players: list[Player], v: Variables) -> list[pulp.LpConstraint]:
        x, captain = v["selected"], v["captain"]
        return [x[p.player_id] - captain[p.player_id] >= 0 for p in players]


# Every constraint that can be sent as JSON, chosen by `type`.
FplConstraint = Annotated[
    Union[
        Size,
        Include,
        IncludeAny,
        Exclude,
        Budget,
        PositionQuota,
        PositionRange,
        MaxPerTeam,
        MaxTransfers,
        OneCaptain,
        CaptainInSquad,
    ],
    Field(discriminator="type"),
]


class Points(Objective):
    """Maximise predicted points. With `captain=True`, the captain's points count twice."""

    type: Literal["points"] = "points"
    captain: bool = False

    def build(self, players: list[Player], v: Variables) -> pulp.LpAffineExpression:
        x = v["selected"]
        if not self.captain:
            return pulp.lpSum(x[p.player_id] * p.predicted_points for p in players)
        captain = v["captain"]
        return pulp.lpSum((x[p.player_id] + captain[p.player_id]) * p.predicted_points for p in players)


def squad(budget: float = 1000.0) -> list[Constraint]:
    """The constraints for the 15-player squad."""
    return [
        Size(name="squad_size", n=15),
        Budget(max=budget),
        PositionQuota(quotas=XV_QUOTAS),
        MaxPerTeam(max=MAX_PER_TEAM),
        OneCaptain(),
        CaptainInSquad(),
    ]


def lineup() -> list[Constraint]:
    """The constraints for the 11-player lineup."""
    return [Size(name="lineup_size", n=11), PositionRange(ranges=XI_RANGES)]


def points(captain: bool) -> Points:
    return Points(captain=captain)
