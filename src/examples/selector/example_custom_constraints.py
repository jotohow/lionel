"""
Example 4: Custom Constraint Demo

Shows how you can define your own constraints for specialized scenarios,
such as "Must include at least 1 player from TeamX" or "At most 10 total
players from any two combined teams", etc.
"""

import pandas as pd
import pulp

from lionel.selector.core.base_selector import BaseSelector


def max_from_team(candidate_df, decision_vars, team_name, max_count=1):
    """
    Example custom constraint: Require at most 'max_count' players from 'team_name'.
    """
    team_idxs = candidate_df.index[candidate_df["team"] == team_name].tolist()
    return pulp.lpSum(decision_vars[i] for i in team_idxs) <= max_count


def main():
    # Generic data
    data = [
        {
            "player": "player_0",
            "team": "team_0",
            "position": "FWD",
            "price": 100,
            "predicted_points": 4.097208871626677,
            "xv": 1,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_1",
            "team": "team_1",
            "position": "FWD",
            "price": 90,
            "predicted_points": 1.817339126752568,
            "xv": 1,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_2",
            "team": "team_2",
            "position": "FWD",
            "price": 55,
            "predicted_points": 3.2342928624644225,
            "xv": 1,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_3",
            "team": "team_3",
            "position": "MID",
            "price": 45,
            "predicted_points": 5.444010315761939,
            "xv": 1,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_4",
            "team": "team_4",
            "position": "MID",
            "price": 100,
            "predicted_points": 3.606503180951658,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_5",
            "team": "team_5",
            "position": "MID",
            "price": 90,
            "predicted_points": 9.654171860889427,
            "xv": 1,
            "xi": 0,
            "captain": 1,
        },
        {
            "player": "player_6",
            "team": "team_6",
            "position": "MID",
            "price": 55,
            "predicted_points": 4.930129576653456,
            "xv": 1,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_7",
            "team": "team_7",
            "position": "MID",
            "price": 45,
            "predicted_points": 6.457452811746765,
            "xv": 1,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_8",
            "team": "team_8",
            "position": "MID",
            "price": 100,
            "predicted_points": 5.4177710895597775,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_9",
            "team": "team_9",
            "position": "MID",
            "price": 90,
            "predicted_points": 3.3611726049239934,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_10",
            "team": "team_0",
            "position": "MID",
            "price": 55,
            "predicted_points": 4.696363780024825,
            "xv": 1,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_11",
            "team": "team_1",
            "position": "DEF",
            "price": 45,
            "predicted_points": 7.144875258968879,
            "xv": 1,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_12",
            "team": "team_2",
            "position": "DEF",
            "price": 100,
            "predicted_points": 6.241213825644516,
            "xv": 1,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_13",
            "team": "team_3",
            "position": "DEF",
            "price": 90,
            "predicted_points": 5.450813398698191,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_14",
            "team": "team_4",
            "position": "DEF",
            "price": 55,
            "predicted_points": 4.044241676260704,
            "xv": 1,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_15",
            "team": "team_5",
            "position": "DEF",
            "price": 45,
            "predicted_points": 2.753491698530036,
            "xv": 1,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_16",
            "team": "team_6",
            "position": "DEF",
            "price": 100,
            "predicted_points": 5.910025215753981,
            "xv": 1,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_17",
            "team": "team_7",
            "position": "DEF",
            "price": 90,
            "predicted_points": 4.468612709807486,
            "xv": 0,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_18",
            "team": "team_8",
            "position": "GK",
            "price": 55,
            "predicted_points": 2.12626863139202,
            "xv": 1,
            "xi": 0,
            "captain": 0,
        },
        {
            "player": "player_19",
            "team": "team_9",
            "position": "GK",
            "price": 45,
            "predicted_points": 1.8208108752164796,
            "xv": 1,
            "xi": 0,
            "captain": 0,
        },
    ]
    candidate_df = pd.DataFrame(data)

    # We'll create a simple BaseSelector for demonstration.
    selector = BaseSelector(candidate_df)

    # Objective: maximize predicted_points
    def objective_func(df, decision_vars):
        return pulp.lpSum(
            decision_vars[i] * df.loc[i, "predicted_points"] for i in range(len(df))
        )

    selector.set_objective_function(objective_func)

    # Add your custom constraint: e.g., must have no more than 1 player from Team_0
    selector.add_constraint(
        lambda df, dvs: max_from_team(df, dvs, "team_0", max_count=1)
    )

    # Additionally, require total count <= 5 (for demonstration)
    selector.add_constraint(lambda df, dvs: pulp.lpSum(dvs) <= 5)

    # Solve
    chosen = selector.select()
    print("Selected players:")
    print(chosen)


if __name__ == "__main__":
    main()
