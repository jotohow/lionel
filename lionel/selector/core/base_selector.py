from collections.abc import Callable, Mapping, Sequence
from typing import Self

import pandas as pd
import pulp
from pydantic import TypeAdapter

from .constraints import Constraint, CoreConstraint, Custom, DecisionVars, Variables
from .objectives import Objective
from .player import Player, PlayerId, players_from_dataframe, validate_players
from .selection import OPTIMAL, Selection

FLAG_FIELDS = ("xv", "xi", "captain")


class Selector:
    """
    Picks players by maximising an objective subject to constraints.

    Usage:
        1) Create the selector from a list of `Player`, or with `from_dataframe()`.
        2) Give it an objective and constraints. Add more with `add_constraint()`.
        3) Call `select()` to solve the problem.

    Constraints are `Constraint` objects, dicts of the same fields (chosen by `type`),
    or callables `(players, x)` for Python authors. `x` maps each `player_id` to its
    binary "selected" variable.

    `flags` maps a variable family to the `Player` flag that marks it in the result,
    for example {"selected": "xv", "captain": "captain"}.
    """

    # Used to validate constraints that are given as dicts. Subclasses add their own types.
    CONSTRAINT_ADAPTER: TypeAdapter = TypeAdapter(CoreConstraint)

    def __init__(
        self,
        players: Sequence[Player | Mapping],
        *,
        objective: Objective,
        constraints: Sequence[Constraint | Mapping | Callable] = (),
        flags: Mapping[str, str] | None = None,
    ) -> None:
        """
        :param players: The candidates, as `Player` or dicts of the same fields.
               Each candidate needs a unique `player_id`.
        :param objective: The sum to maximise.
        :param constraints: The constraints to apply, in order.
        :param flags: Variable family -> `Player` flag field to set in the result.
        """
        self.players: list[Player] = validate_players(players)
        self.num_players: int = len(self.players)
        self.objective = objective
        self.flags: dict[str, str] = dict(flags or {})
        bad = set(self.flags.values()) - set(FLAG_FIELDS)
        if bad:
            raise ValueError(f"flags must map to one of {FLAG_FIELDS}, got {sorted(bad)}")

        self.variables = Variables(self.players)
        self.constraints: list[Constraint] = []
        for constraint in constraints:
            self.add_constraint(constraint)

    @property
    def decision_vars(self) -> DecisionVars:
        """The binary "selected" variable for each player."""
        return self.variables["selected"]

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, columns: Mapping[str, str] | None = None, **kwargs) -> Self:
        """
        Create the selector from a DataFrame, with one row for each candidate.
        :param columns: Maps `Player` field names to column names, for example
               {"player_id": "player_id", "price": "value"}. Unlisted fields use `DEFAULT_COLUMNS`.
        :param kwargs: Passed to the constructor, for example `budget`.
        """
        return cls(players_from_dataframe(df, columns), **kwargs)

    def add_constraint(self, constraint: Constraint | Mapping | Callable) -> None:
        """
        Adds a constraint. A dict is validated by its `type`. A callable is wrapped in `Custom`.
        """
        if callable(constraint) and not isinstance(constraint, Constraint):
            constraint = Custom(fn=constraint)
        elif isinstance(constraint, Mapping):
            constraint = self.CONSTRAINT_ADAPTER.validate_python(dict(constraint))
        self.constraints.append(constraint)

    def select(self) -> Selection:
        """
        Builds the problem, solves it, and returns the result.
        If the solver does not find an optimal selection, `Selection.status` says why
        and `Selection.selected` is empty.
        """
        labels = _unique([c.label() for c in self.constraints])

        # A new problem for each call, so a second call does not repeat the constraints
        self.problem = pulp.LpProblem("SelectorProblem", pulp.LpMaximize)
        self.problem.setObjective(self.objective.build(self.players, self.variables))
        used: set[str] = set()
        for constraint in self.constraints:
            try:
                rows = constraint.apply(self.players, self.variables)
            except (KeyError, ValueError) as e:
                raise ValueError(f"constraint {constraint.label()!r}: {type(e).__name__}: {e}") from e
            names = [constraint.label()] if len(rows) == 1 else [f"{constraint.label()}_{i}" for i in range(len(rows))]
            for name, row in zip(names, rows):
                self.problem.addConstraint(row, _unique([name], used)[0])

        self.problem.solve(pulp.PULP_CBC_CMD(msg=0))

        status = pulp.LpStatus[self.problem.status]
        if status != OPTIMAL:
            return Selection(status=status, candidates=self.players, constraints=labels)

        selected_ids = self._chosen(self.decision_vars)
        flags = {field: self._chosen(self.variables[family]) for family, field in self.flags.items()}
        candidates = [
            p.model_copy(update={field: p.player_id in ids for field, ids in flags.items()}) for p in self.players
        ]
        selected = [p for p in candidates if p.player_id in selected_ids]
        return Selection(
            status=status,
            objective=pulp.value(self.problem.objective),
            selected=selected,
            captain=next((p for p in selected if p.captain), None),
            constraints=labels,
            candidates=candidates,
        )

    @staticmethod
    def _chosen(variables: DecisionVars) -> set[PlayerId]:
        """The ids whose binary variable equals 1 after the solve."""
        return {pid for pid, var in variables.items() if (pulp.value(var) or 0) > 0.5}


BaseSelector = Selector


def _unique(names: list[str], used: set[str] | None = None) -> list[str]:
    """Adds a suffix to repeated names. `used` carries the names between calls."""
    used = set() if used is None else used
    result = []
    for name in names:
        candidate, n = name, 2
        while candidate in used:
            candidate, n = f"{name}_{n}", n + 1
        used.add(candidate)
        result.append(candidate)
    return result
