from lionel.db.connector import DBManager
from lionel.model.hierarchical import FPLPointsModel
from lionel.constants import ANALYSIS, DATA
import lionel.scripts.run_data_load as run_data_load
import lionel.scripts.run_selection as run_selection
import lionel.scripts.run_prediction as run_prediction


def run(season, next_gw, model_path=ANALYSIS / "hm_02.nc"):
    """
    Run the data load, model inference, predictions, and team selections.

    Args:
        season (str): The season for which to run the analysis.
        next_gw (int): The next game week for which to make predictions.

    Returns:
        pandas.DataFrame: The selected team for the next game week.
    """

    # Run the dataload
    _ = run_data_load.run(season)

    # Load model
    fplm = FPLPointsModel.load(model_path)
    dbm = DBManager(DATA / "fpl.db")

    # Put inference data in DB
    df_theta = fplm.summarise_players()
    df_beta = fplm.summarise_teams()
    df_theta.to_sql("player_inference", dbm.engine, if_exists="replace", index=False)
    df_beta.to_sql("team_inference", dbm.engine, if_exists="replace", index=False)

    # Run predictions
    df_pred_next_5 = run_prediction.run(season, next_gw, dbm, fplm)

    # Make team selections
    df_selection = run_selection.run(df_pred_next_5, season, next_gw, dbm)

    return df_selection


if __name__ == "__main__":
    import sys

    n_args = len(sys.argv[1:])
    if n_args == 3:
        season, next_gw, model_path = sys.argv[1:]
    else:
        season, next_gw = sys.argv[1:]
        model_path = ANALYSIS / "hm_02.nc"

    run(int(season), int(next_gw), model_path)
