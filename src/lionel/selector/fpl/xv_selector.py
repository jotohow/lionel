import pandas as pd
import pulp

from ..core.base_selector import BaseSelector


class XVSelector(BaseSelector):
    """
    A concrete subclass for building an FPL squad subject to standard constraints:
      - Budget limit
      - Squad size = 15
      - Max 3 players per team
      - Position minimums (e.g. 2 GKs, 5 DEF, 5 MID, 3 FWD)
      - Exactly 1 captain (doubling predicted points)
      - Objective: maximize sum of (predicted_points) + an additional (predicted_points) for the captain
    """

    # Example distribution. Adjust as needed.
    POS_CONSTRAINTS = {"GK": 2, "DEF": 5, "MID": 5, "FWD": 3}
    MAX_PER_TEAM = 3

    def __init__(
        self,
        candidate_df: pd.DataFrame,
        pred_var: str = "predicted_points",
        budget: float = 1000.0,
    ):
        super().__init__(candidate_df)
        self.pred_var = pred_var
        self.budget = budget

        # Ensure candidate_df has needed columns
        if self.pred_var not in self.candidate_df.columns:
            raise ValueError(f"'{self.pred_var}' not found in candidate_df columns.")
        if "price" not in self.candidate_df.columns:
            raise ValueError("'price' column is required for budget constraints.")
        if "team" not in self.candidate_df.columns:
            raise ValueError("'team' column is required for max-team constraints.")
        if "position" not in self.candidate_df.columns:
            raise ValueError("'position' column is required for position constraints.")

        # Create captain decision variables (one per row)
        # Same integer-based approach as in the base class
        self.captain_vars = [
            pulp.LpVariable(f"capt_{i}", cat=pulp.LpBinary)
            for i in range(self.num_players)
        ]

        # Objective: sum( (x_i + c_i)*pred_var )
        self.set_objective_function(self._objective_with_captains)

        # Add constraints
        self.add_constraint(self._constraint_xv_size)
        self.add_constraint(self._constraint_budget)
        self.add_constraint(self._constraint_positions)
        self.add_constraint(self._constraint_max_team)
        self.add_constraint(self._constraint_exactly_one_captain)
        self.add_constraint(self._constraint_captain_must_be_selected)

    def _objective_with_captains(self, candidate_df, decision_vars):
        """
        Maximize sum of predicted points + an extra predicted_points for the captain.
        i.e. (x_i + c_i)*predicted_points[i].
        """
        return pulp.lpSum(
            (decision_vars[i] + self.captain_vars[i]) * candidate_df.iloc[i][self.pred_var]
            for i in range(self.num_players)
        )

    def _constraint_xv_size(self, candidate_df, decision_vars):
        """Enforce exactly 15 selected players."""
        return pulp.lpSum(decision_vars) == 15

    def _constraint_budget(self, candidate_df, decision_vars):
        """Total cost of selected players must not exceed the budget."""
        return (
            pulp.lpSum(
                decision_vars[i] * candidate_df.iloc[i]["price"]
                for i in range(self.num_players)
            )
            <= self.budget
        )

    def _constraint_positions(self, candidate_df, decision_vars):
        """
        Enforce standard position requirements.
        Example: {'GK':2, 'DEF':5, 'MID':5, 'FWD':3}.
        """
        constraints = []
        for pos, required_count in self.POS_CONSTRAINTS.items():
            # Get the integer-based positions for this 'pos'
            idxs = candidate_df.index[candidate_df["position"] == pos].tolist()
            # Convert index labels -> integer positions
            int_positions = [candidate_df.index.get_loc(label) for label in idxs]
            constraints.append(
                pulp.lpSum(decision_vars[j] for j in int_positions) == required_count
            )
        return constraints

    def _constraint_max_team(self, candidate_df, decision_vars):
        """
        Enforce no more than MAX_PER_TEAM players from the same club.
        """
        constraints = []
        unique_teams = candidate_df["team"].unique()
        for team in unique_teams:
            idxs = candidate_df.index[candidate_df["team"] == team].tolist()
            int_positions = [candidate_df.index.get_loc(label) for label in idxs]
            constraints.append(
                pulp.lpSum(decision_vars[j] for j in int_positions) <= self.MAX_PER_TEAM
            )
        return constraints

    def _constraint_exactly_one_captain(self, candidate_df, decision_vars):
        """Enforce exactly 1 captain among all selected players."""
        return pulp.lpSum(self.captain_vars) == 1

    def _constraint_captain_must_be_selected(self, candidate_df, decision_vars):
        """
        For each player i: x_i >= c_i
        (i.e. a player can't be captain if not in the team).
        """
        constraints = []
        for i in range(self.num_players):
            constraints.append(decision_vars[i] - self.captain_vars[i] >= 0)
        return constraints

    def select(self):
        """
        Solves the optimization problem and returns the chosen subset of candidate_df.
        Also sets 'xv'=1 for selected players, 'captain'=1 for the captain.
        """
        selected_subset = super().select()  # This calls the base solve logic

        # Identify which integer positions were assigned captain
        selected_capt_idxs = [
            i for i in range(self.num_players) if pulp.value(self.captain_vars[i]) == 1
        ]
        # Convert those integer positions back to actual DataFrame labels
        captain_labels = self.candidate_df.index[selected_capt_idxs]

        # Mark columns in the original candidate_df
        self.candidate_df["xv"] = 0
        self.candidate_df.loc[selected_subset.index, "xv"] = 1
        self.candidate_df["captain"] = 0
        self.candidate_df.loc[captain_labels, "captain"] = 1

        # Also mark them in selected_df
        self.selected_df["xv"] = 1
        self.selected_df["captain"] = 0
        # Note: Some rows in selected_df might not be captain
        self.selected_df.loc[captain_labels.intersection(self.selected_df.index), "captain"] = 1

        return self.candidate_df
