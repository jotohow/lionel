"""
Example 2: XI Selection

Demonstrates using XISelector to pick an 11-player starting lineup from a 
subset of 15 players (already chosen in some squad).
"""

import pandas as pd

from lionel.selector.fpl.xi_selector import XISelector


def main():
    # Suppose we have exactly 15 players in a "squad" for which we want to pick 11.
    data = [
        {
            "player": "player_0",
            "team": "team_0",
            "position": "FWD",
            "price": 100,
            "predicted_points": 6.242396935438834,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_1",
            "team": "team_1",
            "position": "FWD",
            "price": 90,
            "predicted_points": 4.051241248413031,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_2",
            "team": "team_2",
            "position": "FWD",
            "price": 55,
            "predicted_points": 4.022555064781975,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_3",
            "team": "team_3",
            "position": "MID",
            "price": 45,
            "predicted_points": 5.58397679263581,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_4",
            "team": "team_4",
            "position": "MID",
            "price": 100,
            "predicted_points": 6.4303264334727235,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_6",
            "team": "team_6",
            "position": "MID",
            "price": 55,
            "predicted_points": 6.231540364580829,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_7",
            "team": "team_7",
            "position": "MID",
            "price": 45,
            "predicted_points": 4.72030026687698,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_10",
            "team": "team_0",
            "position": "MID",
            "price": 55,
            "predicted_points": 7.901694923639182,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_11",
            "team": "team_1",
            "position": "DEF",
            "price": 45,
            "predicted_points": 3.5322658805306735,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_14",
            "team": "team_4",
            "position": "DEF",
            "price": 55,
            "predicted_points": 8.584374446068974,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_15",
            "team": "team_5",
            "position": "DEF",
            "price": 45,
            "predicted_points": 4.700274619709714,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_16",
            "team": "team_6",
            "position": "DEF",
            "price": 100,
            "predicted_points": 3.4691519682328256,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_17",
            "team": "team_7",
            "position": "DEF",
            "price": 90,
            "predicted_points": 10.14355074737708,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_18",
            "team": "team_8",
            "position": "GK",
            "price": 55,
            "predicted_points": 6.600932336744712,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_19",
            "team": "team_9",
            "position": "GK",
            "price": 45,
            "predicted_points": 5.286866640140478,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
    ]
    squad_15_df = pd.DataFrame(data)
    print(squad_15_df)

    # Instantiate an XISelector
    xi_selector = XISelector(squad_15_df, pred_var="predicted_points")

    # Solve / select the best 11
    best_xi = xi_selector.select()
    print(best_xi)
    print("Selected 11-player lineup from the 15-player squad:")
    print(best_xi[best_xi["xi"] == 1])


if __name__ == "__main__":
    main()
