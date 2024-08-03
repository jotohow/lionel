import lionel.team.select as select
import lionel.data_load.storage.storage_handler as storage_handler
from lionel.team.prepare_data import prepare_data


def get_team_choice(df, season):
    pred_vars = [
        "Naive",
        "LGBMRegressor_no_exog",
        "LGBMRegressor_with_exog",
        "LSTMWithReLU",
    ]
    l = []
    for pred_var in pred_vars:
        XVSelector = select.NewXVSelector(
            player_df=df, season=24, pred_var=f"pred_{pred_var}"
        )
        first_xi = XVSelector.pick_xi()
        XVSelector.first_xi = XVSelector.first_xi.rename(
            columns={
                "picked": f"picked_{pred_var}",
                "first_xi": f"first_xi_{pred_var}",
                "captain": f"captain_{pred_var}",
            }
        )
        l.append(XVSelector)

    assert len(set([t.first_xi.shape for t in l])) == 1
    df_1 = l[0].first_xi
    for t, pred_var in zip(l[1:], pred_vars[1:]):
        df_1 = df_1.merge(
            t.first_xi[
                [
                    "unique_id",
                    f"picked_{pred_var}",
                    f"captain_{pred_var}",
                    f"first_xi_{pred_var}",
                ]
            ],
            on="unique_id",
            how="left",
        )
    return df_1


def run(season, next_gw):
    sh = storage_handler.StorageHandler(local=True)
    df = prepare_data(sh, season, next_gw)
    df = get_team_choice(df, season)
    return df


if __name__ == "__main__":
    run(24, 22)
