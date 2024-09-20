import pandas as pd
import numpy as np


def build_selection_data(df_pred, next_gw, fplm):
    points_pred = fplm.predict(df_pred, predictions=True)
    df_pred["points_pred"] = points_pred
    df_pred = df_pred.loc[df_pred.gameweek <= next_gw + 5]
    df_pred["team_name"] = np.where(
        df_pred.is_home, df_pred.home_team, df_pred.away_team
    )
    df_pred = (
        df_pred.groupby(["player", "team_name", "position", "value"])
        .agg(
            mean_points_pred=("points_pred", "mean"),
            next_points_pred=("points_pred", "first"),
        )
        .reset_index()
    )
    df_pred[["xv", "xi", "captain"]] = 0
    return df_pred


def get_scoreline_preds(df_pred, fplm):
    trace = fplm.predict_posterior(df_pred, predictions=True)

    df_ = trace[["home_goals", "away_goals"]].to_dataframe().reset_index(level="match")

    assert (df_.index.get_level_values("chain") == df_.chain).all()
    assert (df_.index.get_level_values("draw") == df_.draw).all()

    df_ = df_.reset_index(drop=True)

    home_teams = fplm.teams[fplm.model.home_team.eval()]
    away_teams = fplm.teams[fplm.model.away_team.eval()]
    match_id = fplm.model.coords["match"]
    df_2 = pd.DataFrame({"home": home_teams, "away": away_teams, "match": match_id})

    df = df_.merge(df_2, on="match", how="left")
    return df
