"""
Example: Using FPLPointsModel for a small dataset.
Shows how the hierarchical points model can be fit and used for posterior predictions.
"""

import numpy as np
import pandas as pd

# Adjust import paths to your actual project structure
from lionel.model.bayesian.hierarchical_model import HierarchicalPointsModel


def main():
    # Small dataset with two "gameweeks" worth of data
    # (6 players, repeated for 2 gameweeks, total 12 rows).
    player = ["player_1", "player_2", "player_3", "player_4", "player_5", "player_6"]
    gameweek = [1] * len(player) + [2] * len(player)
    season = [25] * len(player) * 2
    home_team = ["team_1"] * len(player) + ["team_2"] * len(player)
    away_team = ["team_2"] * len(player) + ["team_1"] * len(player)
    home_goals = [1] * len(player) + [2] * len(player)
    away_goals = [0] * len(player) + [1] * len(player)
    position = ["FWD", "MID", "DEF", "GK", "FWD", "MID"] * 2
    minutes = [90] * len(player) * 2
    goals_scored = [1, 0, 0, 0, 0, 0] + [0, 0, 1, 1, 0, 1]
    assists = [0, 1, 0, 0, 0, 0] + [1, 0, 0, 0, 1, 1]
    is_home = [True, True, True, False, False, False] + [
        False,
        False,
        False,
        True,
        True,
        True,
    ]
    points = [10, 6, 2, 2, 2, 2] + [6, 2, 10, 10, 2, 10]

    df = pd.DataFrame(
        {
            "player": player + player,
            "gameweek": gameweek,
            "season": season,
            "home_team": home_team,
            "away_team": away_team,
            "home_goals": home_goals,
            "away_goals": away_goals,
            "position": position,
            "minutes": minutes,
            "goals_scored": goals_scored,
            "assists": assists,
            "is_home": is_home,
        }
    )

    # Initialize the model
    fpl_model = HierarchicalPointsModel()

    # Fit the model with the small dataset
    # 'points' is the actual FPL points from the array above
    fpl_model.fit(df, np.array(points), progressbar=True)

    # Generate predictions (posterior point estimates, etc.)
    # For a Bayesian model, you might want to get posterior draws or point predictions.
    preds = fpl_model.predict(df, extend_idata=False, predictions=True)
    df["predicted_points"] = preds
    print(df)


if __name__ == "__main__":
    main()
