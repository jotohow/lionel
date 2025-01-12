"""
Example 3: Updating an Existing XV

Demonstrates using UpdateXVSelector to modify a previously selected squad 
(by making up to 'max_transfers' changes) while respecting budget, positions, 
and captain logic.
"""

import pandas as pd

from lionel.selector.fpl.xv_selector import UpdateXVSelector


def main():
    # Suppose we have a DataFrame representing our existing squad of 15 (xv=1)
    # plus additional players who could potentially be transferred in (xv=0).
    # Must have exactly 15 with xv=1.

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
            "xv": 0,
            "xi": 0,
            "captain": 0,
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
            "xv": 1,
            "xi": 0,
            "captain": 1,
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
    full_df = pd.DataFrame(data)

    # Instantiate with a max of 2 transfers allowed
    updater = UpdateXVSelector(
        candidate_df=full_df, max_transfers=2, pred_var="predicted_points", budget=1000
    )

    # Solve: tries to swap up to 2 new players to maximize total points
    updated_squad = updater.select()

    print("Updated squad after at most 2 transfers:")
    print(updated_squad[updated_squad["xv"] == 1])
    print("\nCaptain chosen:")
    print(updated_squad[updated_squad["captain"] == 1])


if __name__ == "__main__":
    main()
