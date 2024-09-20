import lionel.selector as selector
import lionel.data_load.process.process_train_data as process_train_data
import lionel.model.prepare_data as prepare_data


def run(season, next_gw, dbm, fplm):
    df = process_train_data.get_train(dbm, season, next_gw)
    df_pred = process_train_data.get_pred_dataset(dbm, df, season, next_gw)
    df_team = prepare_data.build_selection_data(df_pred, next_gw, fplm)

    # Run selections for XI and XV
    xvsel = selector.NewXVSelector("mean_points_pred")
    xvsel.build_problem(df_team)
    xvsel.solve()

    xisel = selector.XISelector("mean_points_pred")
    xisel.build_problem(df_team)
    xisel.solve()

    df_selection = xisel.data.copy()
    df_selection["gameweek"] = next_gw

    df_scoreline = prepare_data.get_scoreline_preds(df_pred, fplm)

    # next gw games
    df_next = (
        df_pred[df_pred.gameweek == 5][["home_team", "away_team"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return df_selection, df_scoreline, df_next


if __name__ == "__main__":
    run(24, 22)
