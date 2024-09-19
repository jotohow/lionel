import pandas as pd

# import lionel.data_load.storage_handler as storage_handler


def add_opponent_names(df_gw_stats, df_team_ids):
    df_team_map = df_team_ids[["team_id", "team_name"]].rename(
        columns={"team_id": "opponent_team", "team_name": "opponent_team_name"}
    )
    df_gw_stats = df_gw_stats.merge(df_team_map, on="opponent_team", how="left")
    return df_gw_stats
