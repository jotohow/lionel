import numpy as np
import pandas as pd

# from connect.db import PostgresConnector


class BetFPLCombiner:

    def __init__(
        self,
        next_gameweek,
        df_odds,  # adjusted these to be passed instead of collected from DB
        df_players,
        season=24,
        odds_weight_def=0.6,
        odds_weight_fwd=0.4,
    ):
        self.season = season
        self.gameweek = next_gameweek
        self.odds_weight_def = odds_weight_def
        self.odds_weight_fwd = odds_weight_fwd
        self.df_next_game = pd.DataFrame()
        self.df_odds = df_odds
        self.df_players = df_players

    def prepare_next_gw(self):
        df_next_game = self._shape_home_away_fixtures(self.df_players, self.df_odds)
        df_next_game = self._shape_double_gameweeks(df_next_game)
        df_next_game = self._clean_next_game(df_next_game, self.gameweek)
        self.df_next_game = df_next_game
        return df_next_game

    @staticmethod
    def _shape_home_away_fixtures(df_players, df_odds):
        next_home_df = df_players[df_players["is_home"]]
        next_away_df = df_players[~df_players["is_home"]]

        df_odds[["home", "away"]] = df_odds[["home", "away"]].apply(
            lambda x: x.str.strip(), axis=0
        )

        if df_odds["season"].dtype != "int64":
            df_odds["season"] = df_odds["season"].str.slice(start=-2).astype(int)

        next_home_df = next_home_df.merge(
            df_odds,
            left_on=["team_name", "season", "next_opponent_name"],
            right_on=["home", "season", "away"],
            how="left",
        )
        next_away_df = next_away_df.merge(
            df_odds,
            left_on=["team_name", "season", "next_opponent_name"],
            right_on=["away", "season", "home"],
            how="left",
        )
        df_next_game = pd.concat([next_home_df, next_away_df])
        return df_next_game

    @staticmethod
    def _shape_double_gameweeks(df_next_game):

        COLS = [
            "name",
            "element",
            "team_name",
            "total_points",
            "value",
            "ict_index",
            "minutes",
            "position",
        ]
        COLS_2 = COLS + [
            "is_home",
            "home_odds",
            "draw_odds",
            "away_odds",
            "home",
            "away",
        ]
        pivot = df_next_game[COLS_2]

        # Account for gameweeks with two games
        g = pivot.groupby(COLS).cumcount().add(1)
        pivot = pivot.set_index(COLS + [g]).unstack().sort_index(axis=1, level=1)
        pivot.columns = ["{}{}".format(a, b) for a, b in pivot.columns]
        pivot = pivot.reset_index()
        pivot["win_odds1"] = np.where(
            pivot["home1"] == pivot["team_name"],
            pivot["home_odds1"],
            pivot["away_odds1"],
        )

        # Aggregate win odds where there are twp games
        if "home2" in pivot.columns:
            pivot["win_odds2"] = np.where(
                pivot["home2"] == pivot["team_name"],
                pivot["home_odds2"],
                pivot["away_odds2"],
            )

            # Arbitrarily add 20% because the players still get more game time for points even if they are unlikely to win either.
            pivot["agg_win_odds"] = np.where(
                ~pivot["home2"].isna(),
                (
                    (pivot["win_odds1"] + pivot["win_odds2"])
                    - (pivot["win_odds1"] * pivot["win_odds2"])
                ),
                pivot["win_odds1"],
            )

        else:
            pivot["agg_win_odds"] = pivot["win_odds1"]

        return pivot

    def _weight_points(self, df_next_game):
        df_next_game["points_weighted"] = np.where(
            df_next_game.position.isin(["DEF", "GK"]),
            ((1 - self.odds_weight_def) * df_next_game["scaled_points"])
            + (self.odds_weight_def * df_next_game["agg_win_odds"]),
            ((1 - self.odds_weight_fwd) * df_next_game["scaled_points"])
            + (self.odds_weight_fwd * df_next_game["agg_win_odds"]),
        )
        return df_next_game

    def _clean_next_game(self, df_next_game, next_gameweek):

        df_next_game["games_played"] = df_next_game["minutes"] / 90
        df_next_game["scaled_points"] = (
            df_next_game["total_points"] / df_next_game["total_points"].max()
        )
        df_next_game["next_gw"] = next_gameweek

        df_next_game = self._weight_points(df_next_game)

        df_next_game["next_opp"] = np.where(
            df_next_game.team_name == df_next_game.away1,
            df_next_game.home1,
            df_next_game.away1,
        )
        df_next_game["surname"] = df_next_game.name.apply(lambda x: x.split()[-1])
        return df_next_game
