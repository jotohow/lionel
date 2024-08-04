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
    preds = lionel.model.run.run(sh, df_train, season, next_gw, gw_horizon)

    # Run team selection
    df_team = lionel.team.run.run(sh, season, next_gw)


if __name__ == "__main__":
    run(*[int(x) for x in sys.argv[1:]])
