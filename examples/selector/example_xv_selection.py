"""
Example 1: Basic XV Selection

Demonstrates how to use XVSelector to select a 15-player squad from a larger
candidate pool, subject to budget, position, team, and captain constraints.
"""

import numpy as np
import pandas as pd

from lionel.selector.fpl.xv_selector import XVSelector


def main():
    # Suppose we have a dataset of ~200 players with columns:
    # ['player_id', 'name', 'team', 'position', 'price', 'predicted_points']
    df = pd.DataFrame(
        {
            "player": [f"player_{i}" for i in range(20)],
            "team": [f"team_{i%10}" for i in range(20)],
            "position": ["FWD"] * 3 + ["MID"] * 8 + ["DEF"] * 7 + ["GK"] * 2,
            "price": [
                100,
                90,
                55,
                45,
                100,
                90,
                55,
                45,
                100,
                90,
                55,
                45,
                100,
                90,
                55,
                45,
                100,
                90,
                55,
                45,
            ],
            "predicted_points": np.random.normal(5, 2, 20),
            "xv": [0] * 20,
            "xi": [0] * 20,
            "captain": [0] * 20,
        }
    )

    # Create an XVSelector with a default budget
    xv_selector = XVSelector(df, pred_var="predicted_points", budget=1000)

    # Solve / select the squad
    best_squad = xv_selector.select()

    print("Selected 15-player squad:")
    print(best_squad)
    print("\nCaptain chosen:")
    print(best_squad[best_squad["captain"] == 1])


if __name__ == "__main__":
    main()
