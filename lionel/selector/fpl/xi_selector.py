# xi_selector.py

import pandas as pd
import pulp

from ..core.base_selector import BaseSelector


class XISelector(BaseSelector):
    """
    Class for selecting a team of 11 players from a candidate set (e.g., the 15 already in a squad).

    This class uses BaseSelector's methods to:
      - Maximize a column of predicted points (pred_var).
      - Enforce exactly 11 selections.
      - Enforce position constraints (using POS_CONSTRAINTS).
    """

    # Minimum and maximum number of each position allowed
    POS_CONSTRAINTS = {
        "GK": (1, 1),  # exactly 1 GK
        "DEF": (3, 5),
        "MID": (2, 5),
        "FWD": (1, 3),
    }

    def __init__(self, candidate_df: pd.DataFrame, pred_var: str = "predicted_points"):
        """
        Initializes an XISelector object.

        Args:
            candidate_df (pd.DataFrame): The candidate set of players (typically 15 from a bigger squad).
            pred_var (str): The name of the variable/column used for prediction.
        """
        super().__init__(candidate_df)
        self.pred_var = pred_var

        # Ensure candidate_df has the prediction column
        if self.pred_var not in self.candidate_df.columns:
            raise ValueError(f"'{self.pred_var}' not found in candidate_df columns.")

        # Set default objective to maximize sum of pred_var
        self.set_objective_function(self.default_objective)

        # Add constraints: exactly 11 players, plus position constraints
        self.add_constraint(self.constraint_xi_size)
        self.add_constraint(self.constraint_positions)

    def default_objective(self, candidate_df, decision_vars):
        """
        By default, maximize the sum of pred_var for selected players.
        """
        return pulp.lpSum(
            [
                decision_vars[i] * candidate_df.loc[i, self.pred_var]
                for i in range(len(candidate_df))
            ]
        )

    def constraint_xi_size(self, candidate_df, decision_vars):
        """
        Enforces exactly 11 selected players.
        """
        return pulp.lpSum(decision_vars) == 11

    def constraint_positions(self, candidate_df, decision_vars):
        """
        Enforces position constraints:
          - Each position must be between its defined min and max.
          - Requires a 'position' column in candidate_df.
        """
        if "position" not in candidate_df.columns:
            raise ValueError(
                "'candidate_df' must have a 'position' column for XISelector constraints."
            )

        constraints = []
        for pos, (min_pos, max_pos) in self.POS_CONSTRAINTS.items():
            pos_indices = candidate_df[candidate_df["position"] == pos].index.tolist()
            if (
                pos_indices
            ):  # Only add constraints if the position actually exists in the data
                constraints.append(
                    pulp.lpSum([decision_vars[i] for i in pos_indices]) >= min_pos
                )
                constraints.append(
                    pulp.lpSum([decision_vars[i] for i in pos_indices]) <= max_pos
                )
        return constraints

    def select(self):

        # 1) Solve the problem
        selected_subset = super().select()

        # 2) Mark columns
        self.candidate_df["xi"] = 0
        self.candidate_df.loc[selected_subset.index, "xi"] = 1

        return self.candidate_df.loc[self.candidate_df["xi"] == 1]
