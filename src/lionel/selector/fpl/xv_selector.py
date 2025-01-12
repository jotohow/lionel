# xv_selector.py

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
        """
        Initializes the XVSelector with an additional captain variable.

        Args:
            candidate_df (pd.DataFrame): The candidate set of players.
            pred_var (str): Column name for predicted points.
            budget (float): The total budget available.
        """
        super().__init__(candidate_df)
        self.pred_var = pred_var
        self.budget = budget

        # Ensure candidate_df has the needed columns
        if self.pred_var not in candidate_df.columns:
            raise ValueError(f"'{self.pred_var}' not found in candidate_df columns.")
        if "price" not in candidate_df.columns:
            raise ValueError("'price' column is required for budget constraints.")
        if "team" not in candidate_df.columns:
            raise ValueError("'team' column is required for max-team constraints.")
        if "position" not in candidate_df.columns:
            raise ValueError("'position' column is required for position constraints.")

        # Create captain decision variables (one per row)
        self.captain_vars = [
            pulp.LpVariable(f"capt_{i}", cat=pulp.LpBinary)
            for i in range(self.num_players)
        ]

        # Objective: sum( (x_i + captain_i)*pred_var )
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
            (decision_vars[i] + self.captain_vars[i])
            * candidate_df.loc[i, self.pred_var]
            for i in range(len(candidate_df))
        )

    def _constraint_xv_size(self, candidate_df, decision_vars):
        """Enforce exactly 15 selected players."""
        return pulp.lpSum(decision_vars) == 15

    def _constraint_budget(self, candidate_df, decision_vars):
        """Total cost of selected players must not exceed the budget."""
        return (
            pulp.lpSum(
                decision_vars[i] * candidate_df.loc[i, "price"]
                for i in range(len(candidate_df))
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
            idxs = candidate_df[candidate_df["position"] == pos].index.tolist()
            constraints.append(
                pulp.lpSum(decision_vars[i] for i in idxs) == required_count
            )
        return constraints

    def _constraint_max_team(self, candidate_df, decision_vars):
        """
        Enforce no more than MAX_PER_TEAM players from the same club.
        """
        constraints = []
        unique_teams = candidate_df["team"].unique()
        for team in unique_teams:
            idxs = candidate_df[candidate_df["team"] == team].index.tolist()
            constraints.append(
                pulp.lpSum(decision_vars[i] for i in idxs) <= self.MAX_PER_TEAM
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
        for i in range(len(candidate_df)):
            constraints.append(decision_vars[i] - self.captain_vars[i] >= 0)
        return constraints

    def select(self):
        """
        Solves the optimization problem and returns the chosen subset of candidate_df.
        Also sets 'xv'=1 for selected players, 'captain'=1 for the captain.
        """
        selected_subset = super().select()  # This calls the base solve logic

        # Identify the captain
        selected_capt_idxs = [
            i for i, cap_var in enumerate(self.captain_vars) if pulp.value(cap_var) == 1
        ]

        # Mark columns
        self.candidate_df["xv"] = 0
        self.candidate_df.loc[selected_subset.index, "xv"] = 1
        self.selected_df["xv"] = 1
        self.selected_df["captain"] = 0
        self.selected_df.loc[selected_capt_idxs, "captain"] = 1

        self.candidate_df["captain"] = 0
        self.candidate_df.loc[selected_capt_idxs, "captain"] = 1

        return self.candidate_df.loc[self.candidate_df["xv"] == 1]


class UpdateXVSelector(XVSelector):
    """
    Subclass that allows updating an existing XV by making a limited number of transfers.

    Expects the input DataFrame to have:
      - 'xv' column with exactly 15 players set to 1 (the existing squad).
      - 'price', 'team', 'position', 'predicted_points' (or your chosen pred_var).
      - Possibly 'captain' if you want to track an existing captain, though
        reassigning captains may be decided by the solver.

    Adds one extra constraint:
      - You can only add up to 'max_transfers' new players (i.e., those who previously had xv=0).
    """

    def __init__(
        self,
        candidate_df: pd.DataFrame,
        max_transfers: int = 1,
        pred_var: str = "predicted_points",
        budget: float = 1000.0,
    ):
        """
        Initialize UpdateXVSelector with the same logic as XVSelector,
        but add a constraint for the maximum number of new players to bring in.

        Args:
            candidate_df (pd.DataFrame): Must have exactly 15 players with xv=1.
            max_transfers (int): The max number of new players allowed to be added.
            pred_var (str): Column name for predicted points.
            budget (float): The total budget available.
        """
        super().__init__(candidate_df, pred_var=pred_var, budget=budget)
        self.max_transfers = max_transfers

        # Validate that the existing team has exactly 15 players selected
        if "xv" not in self.candidate_df.columns:
            raise ValueError(
                "candidate_df must have an 'xv' column to track existing squad."
            )
        if self.candidate_df["xv"].sum() != 15:
            raise ValueError(
                "The existing squad must have exactly 15 players set to 'xv=1'."
            )

        # Add the constraint for the maximum number of new players
        self.add_constraint(self._constraint_max_transfers)

    def _constraint_max_transfers(self, candidate_df, decision_vars):
        """
        For players who currently have xv=0 (not in the existing squad),
        limit how many of those can be added to at most 'max_transfers'.
        """
        new_player_idxs = candidate_df.index[candidate_df["xv"] == 0].tolist()
        return (
            pulp.lpSum([decision_vars[i] for i in new_player_idxs])
            <= self.max_transfers
        )
