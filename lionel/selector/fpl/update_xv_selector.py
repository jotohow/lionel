import pandas as pd
import pulp

from .xv_selector import XVSelector


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
        super().__init__(candidate_df, pred_var=pred_var, budget=budget)
        self.max_transfers = max_transfers

        # Validate that the existing team has exactly 15 players selected
        if "xv" not in self.candidate_df.columns:
            raise ValueError("candidate_df must have an 'xv' column to track existing squad.")
        if self.candidate_df["xv"].sum() != 15:
            raise ValueError("The existing squad must have exactly 15 players set to 'xv=1'.")

        # Add the constraint for the maximum number of new players
        self.add_constraint(self._constraint_max_transfers)

    def _constraint_max_transfers(self, candidate_df, decision_vars):
        """
        For players who currently have xv=0 (not in the existing squad),
        limit how many of those can be added to at most 'max_transfers'.
        """
        # Identify row labels where xv==0
        new_player_labels = candidate_df.index[candidate_df["xv"] == 0].tolist()
        # Convert those labels to integer positions
        new_player_positions = [candidate_df.index.get_loc(lbl) for lbl in new_player_labels]

        return pulp.lpSum([decision_vars[i] for i in new_player_positions]) <= self.max_transfers
