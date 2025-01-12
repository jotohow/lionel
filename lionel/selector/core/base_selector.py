import pandas as pd
import pulp


class BaseSelector:
    """
    A generic selector class for building optimization solutions.

    Usage:
        1) Provide a dataframe of 'players' or items to choose from.
        2) Override or call `set_constraints()` to add constraints.
        3) Define or override an objective function with `set_objective_function()`.
        4) Call `select()` to solve the optimization problem.

    Subclasses should override or extend these methods for specific logic.
    """

    def __init__(self, candidate_df: pd.DataFrame):
        """
        :param candidate_df: DataFrame containing all candidate items (e.g. players).
               Must have a unique identifier for each row (e.g. player_id).
        """
        self.candidate_df = candidate_df.reset_index(drop=True)
        self.selected_df = pd.DataFrame(columns=self.candidate_df.columns)
        self.num_players = len(self.candidate_df)

        # Create a binary decision variable x_i for each row/player
        self.decision_vars = [
            pulp.LpVariable(f"x_{i}", cat=pulp.LpBinary)
            for i in range(self.num_players)
        ]

        # Create the base problem
        self.problem = pulp.LpProblem("GenericSelectorProblem", pulp.LpMaximize)

        # Default objective and constraints are empty
        self.objective_func = None
        self.custom_constraints = []

    # is this needed? It's just masking smoething basic..
    def set_objective_function(self, objective_func):
        """
        Sets the objective function for the solver.
        :param objective_func: A callable that takes (candidate_df, decision_vars)
                               and returns a PuLP expression.
        """
        self.objective_func = objective_func

    def add_constraint(self, constraint_func):
        """
        Adds a constraint to the solver.
        :param constraint_func: A callable that takes (candidate_df, decision_vars)
                                and returns a PuLP constraint or list of constraints.
        """
        self.custom_constraints.append(constraint_func)

    def select(self):
        """
        Finalizes the objective & constraints, solves the problem, and returns
        the chosen items (players).
        :return: A subset of candidate_df that were selected by the solver.
        """
        # 1) Set the objective if provided
        if self.objective_func is None:
            raise ValueError(
                "No objective function has been set. Call set_objective_function() first."
            )

        self.problem.setObjective(
            self.objective_func(self.candidate_df, self.decision_vars)
        )

        # 2) Add all custom constraints
        for cfunc in self.custom_constraints:
            constraints = cfunc(self.candidate_df, self.decision_vars)
            if isinstance(constraints, list):
                # If the function returns multiple constraints
                for con in constraints:
                    self.problem.addConstraint(con)
            else:
                # If a single constraint is returned
                self.problem.addConstraint(constraints)

        # 3) Solve the problem
        self.problem.solve(pulp.PULP_CBC_CMD(msg=0))

        # 4) Collect selected players
        selected_indices = [
            i for i, var in enumerate(self.decision_vars) if pulp.value(var) == 1
        ]
        self.selected_df = self.candidate_df.loc[selected_indices].copy()
        return self.selected_df
