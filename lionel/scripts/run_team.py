import lionel.selector as selector
import lionel.data_load.process.process_train_data as process_train_data
import lionel.model.prepare_data as prepare_data


# def get_team_choice(df, season):
#     """
#     Returns a DataFrame containing the team choices for each prediction variable.

#     Args:
#         df (pandas.DataFrame): The player DataFrame.
#         season (int): The season number.

#     Returns:
#         pandas.DataFrame: The DataFrame containing the team choices for each prediction variable.
#     """

#     pred_vars = [
#         "Naive",
#         "LGBMRegressor_no_exog",
#         "LGBMRegressor_with_exog",
#         "LSTMWithReLU",
#     ]
#     l = []
#     for pred_var in pred_vars:
#         XVSelector = select.NewXVSelector(
#             player_df=df, season=24, pred_var=f"pred_{pred_var}"
#         )
#         first_xi = XVSelector.pick_xi()
#         XVSelector.first_xi = XVSelector.first_xi.rename(
#             columns={
#                 "picked": f"picked_{pred_var}",
#                 "first_xi": f"first_xi_{pred_var}",
#                 "captain": f"captain_{pred_var}",
#             }
#         )
#         l.append(XVSelector)

#     assert len(set([t.first_xi.shape for t in l])) == 1
#     df_1 = l[0].first_xi
#     for t, pred_var in zip(l[1:], pred_vars[1:]):
#         df_1 = df_1.merge(
#             t.first_xi[
#                 [
#                     "unique_id",
#                     f"picked_{pred_var}",
#                     f"captain_{pred_var}",
#                     f"first_xi_{pred_var}",
#                 ]
#             ],
#             on="unique_id",
#             how="left",
#         )
#     df_1["season"] = season
#     return df_1


# # TODO: Add in current values
# # def run(sh, season, next_gw):
# #     """
# #     Runs the team selection process for a given season and next game week.

# #     Args:
# #         season (str): The season for which the team selection is being done.
# #         next_gw (int): The next game week for which the team selection is being done.

# #     Returns:
# #         pandas.DataFrame: The resulting DataFrame containing the team selection.

# #     """
# #     # sh = storage_handler.StorageHandler(local=True)
# #     df_preds = sh.load(f"analysis/preds_{next_gw}_{season}.csv")
# #     df_train = sh.load(f"analysis/train_{next_gw}_{season}.csv")

# #     df = prepare_data(df_train, df_preds, season, next_gw)
# #     df = get_team_choice(df, season)

# #     sh.store(df, f"analysis/team_selection_{next_gw}_{season}.csv", index=False)
# #     return df


def run(season, next_gw, dbm, fplm):
    df = process_train_data.get_train(dbm, season, next_gw)
    df_pred = process_train_data.get_pred_dataset(dbm, df, season, next_gw)
    df_team = prepare_data.build_selection_data(df_pred, next_gw, fplm)

    # next gw games
    df_next = (
        df_pred[df_pred.gameweek == 5][["home_team", "away_team"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    xvselector = selector.NewXVSelector(
        player_df=df_team, season=season, pred_var="mean_points_pred"
    )
    xvselector.pick_xi()
    df_selection = xvselector.first_xi.copy()
    df_selection["gameweek"] = next_gw

    df_scoreline = prepare_data.get_scoreline_preds(df_pred, fplm)
    return df_selection, df_scoreline, df_next


if __name__ == "__main__":
    run(24, 22)
