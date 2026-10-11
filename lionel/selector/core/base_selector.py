from collections.abc import Callable, Mapping, Sequence
from typing import Self

import pandas as pd
import pulp

from .player import Player, PlayerId, players_from_dataframe, validate_players
from .selection import OPTIMAL, Selection

DecisionVars = dict[PlayerId, pulp.LpVariable]
ObjectiveFunc = Callable[[list[Player], DecisionVars], pulp.LpAffineExpression]
ConstraintFunc = Callable[[list[Player], DecisionVars], pulp.LpConstraint | list[pulp.LpConstraint]]


class BaseSelector:
    """
    A generic selector class for building optimization solutions.

    Usage:
        1) Create the selector from a list of `Player`, or with `from_dataframe()`.
        2) Call `add_constraint()` to add constraints.
        3) Define or override an objective function with `set_objective_function()`.
        4) Call `select()` to solve the optimization problem.

    Objective and constraint callables take `(players, x)`. `players` is the
    list of `Player` in input order. `x` maps each `player_id` to its binary
    decision variable.

    Subclasses should override or extend these methods for specific logic.
    """

    def __init__(self, players: Sequence[Player | Mapping]) -> None:
        """
        :param players: The candidates, as `Player` or dicts of the same fields.
               Each candidate needs a unique `player_id`.
        """
        self.players: list[Player] = validate_players(players)
        self.num_players: int = len(self.players)

        # Create a binary decision variable x_i for each player, keyed by player_id
        self.decision_vars: DecisionVars = {
            p.player_id: pulp.LpVariable(f"x_{i}", cat=pulp.LpBinary) for i, p in enumerate(self.players)
        }

        # Default objective and constraints are empty
        self.objective_func: ObjectiveFunc | None = None
        self.custom_constraints: list[ConstraintFunc] = []

    @classmethod
    def from_dataframe(cls, df: pd.DataFrame, columns: Mapping[str, str] | None = None, **kwargs) -> Self:
        """
        Create the selector from a DataFrame, with one row for each candidate.
        :param columns: Maps `Player` field names to column names, for example
               {"player_id": "player_id", "price": "value"}. Unlisted fields use `DEFAULT_COLUMNS`.
        :param kwargs: Passed to the constructor, for example `budget`.
        """
        return cls(players_from_dataframe(df, columns), **kwargs)

    def set_objective_function(self, objective_func: ObjectiveFunc) -> None:
        """
        Sets the objective function for the solver.
        :param objective_func: A callable that takes (players, decision_vars)
                               and returns a PuLP expression.
        """
        self.objective_func = objective_func

    def add_constraint(self, constraint_func: ConstraintFunc) -> None:
        """
        Adds a constraint to the solver.
        :param constraint_func: A callable that takes (players, decision_vars)
                                and returns a PuLP constraint or list of constraints.
        """
        self.custom_constraints.append(constraint_func)

    def select(self) -> Selection:
        """
        Builds the problem, solves it, and returns the result.
        If the solver does not find an optimal selection, `Selection.status` says why
        and `Selection.selected` is empty.
        """
        if self.objective_func is None:
            raise ValueError("No objective function has been set. Call set_objective_function() first.")

        # A new problem for each call, so a second call does not repeat the constraints
        self.problem = pulp.LpProblem("GenericSelectorProblem", pulp.LpMaximize)
        self.problem.setObjective(self.objective_func(self.players, self.decision_vars))
        for cfunc in self.custom_constraints:
            constraints = cfunc(self.players, self.decision_vars)
            if isinstance(constraints, list):
                for con in constraints:
                    self.problem.addConstraint(con)
            else:
                self.problem.addConstraint(constraints)

        self.problem.solve(pulp.PULP_CBC_CMD(msg=0))

        status = pulp.LpStatus[self.problem.status]
        if status != OPTIMAL:
            return Selection(status=status, candidates=self.players)

        selected_ids = self._chosen(self.decision_vars)
        flags = self._flags(selected_ids)
        candidates = [
            p.model_copy(update={field: p.player_id in ids for field, ids in flags.items()}) for p in self.players
        ]
        selected = [p for p in candidates if p.player_id in selected_ids]
        return Selection(
            status=status,
            objective=pulp.value(self.problem.objective),
            selected=selected,
            captain=next((p for p in selected if p.captain), None),
            candidates=candidates,
        )

    def _flags(self, selected_ids: set[PlayerId]) -> dict[str, set[PlayerId]]:
        """
        Maps each `Player` flag field to the ids that get the flag.
        Fields that are not in the result keep their input value.
        """
        return {}

    @staticmethod
    def _chosen(variables: DecisionVars) -> set[PlayerId]:
        """The ids whose binary variable equals 1 after the solve."""
        return {pid for pid, var in variables.items() if (pulp.value(var) or 0) > 0.5}
