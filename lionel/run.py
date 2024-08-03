import sys
from lionel.data_load.storage.storage_handler import StorageHandler
import lionel.data_load.run
import lionel.model.run


def run(season, next_gw, gw_horizon=5):
    print(season, next_gw, gw_horizon)
    sh = StorageHandler(local=True)

    # Run data load
    df_train = lionel.data_load.run.run(sh, season, next_gw)

    # Run predictions
    preds = lionel.model.run.run(df_train, season, next_gw, gw_horizon=gw_horizon)
    sh.store(preds, f"analysis/preds_{next_gw}_{season}.csv", index=False)


if __name__ == "__main__":
    run(*[int(x) for x in sys.argv[1:]])
