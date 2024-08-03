import sys
from lionel.data_load.storage.storage_handler import StorageHandler
import lionel.data_load.run
import lionel.model.run
import lionel.team.run


def run(season, next_gw, gw_horizon=5):
    """
    Run the process for Lionel.

    Run the data load, model predictions and team selection for the given season and next game week. Store everything (locally by default).

    Args:
        season (str): The season of the data.
        next_gw (int): The next game week.
        gw_horizon (int, optional): The number of game weeks to consider for predictions. Defaults to 5.
    """
    sh = StorageHandler(local=True)

    # Run data load
    df_train = lionel.data_load.run.run(sh, season, next_gw)

    # Run predictions
    preds = lionel.model.run.run(df_train, season, next_gw, gw_horizon=gw_horizon)
    sh.store(preds, f"analysis/preds_{next_gw}_{season}.csv", index=False)

    # Run team selection
    df_team = lionel.team.run.run(season, next_gw)
    sh.store(df_team, f"analysis/team_selection_{next_gw}_{season}.csv")


if __name__ == "__main__":
    run(*[int(x) for x in sys.argv[1:]])
