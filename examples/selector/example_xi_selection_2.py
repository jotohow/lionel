"""
Example 5: Select XI after XV selection

"""

import numpy as np
import pandas as pd

from lionel.selector.fpl.xi_selector import XISelector
from lionel.selector.fpl.xv_selector import XVSelector


def main():
    # Suppose we have a dataset of ~200 players with columns:
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

    # Now select XI from the XV
    xi_selector = XISelector(best_squad.loc[best_squad["xv"] == 1])
    xi_selector.select()
    # print(xi_selector.selected_df)

    # Then index everything back to the original DataFrame
    xi_index = xi_selector.selected_df.index
    best_squad.loc[xi_index, "xi"] = 1
    best_squad = best_squad.sort_values(by=["captain", "xi", "xv"], ascending=[False, False, False])
    print(best_squad)


if __name__ == "__main__":
    main()
