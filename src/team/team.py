from typing import Type
import pandas as pd
import numpy as np

from team.select import NewXVSelector, UpdateXVSelector


""" 
Steps to enable updating from an existing Team:
1. Allow player ids to be passed in
2. Get the data for those players from dataframe etc as normal
3. Run optimisation with current players as a starting point
"""


class Team:
    """
    Represents a team for a specific season and gameweek.

    Attributes:
        season (str): The season of the team.
        gameweek (int): The gameweek of the team.
        odds_weight_def (float): The weight for defensive player odds.
        odds_weight_fwd (float): The weight for forward player odds.
        selector (Type["Team"]): The selector object for picking the team.
        df_next_game (pd.DataFrame): The dataframe containing next gameweek data.
        initial_xi (Type["Team"]): The previous team for updates to be made to.
        first_xi (pd.DataFrame): The first eleven players selected for the team.
        first_xv (pd.DataFrame): The first eleven players' values.
        processor (Any): The processor object for preparing next gameweek data.
        budget (int): The budget available for the team.
        value (int): The total value of the first eleven players.
        selected (bool): Indicates if the team has been selected.
        testing (bool): Indicates if the team is being tested.
    """

    def __init__(
        self,
        season,
        next_gameweek,  # adjusted from current_gameweek to next_gameweek
        df_next_game=pd.DataFrame(),
        initial_xi=[],  # The previous team for updates to be made to
        odds_weight_def=0.6,
        odds_weight_fwd=0.4,
        budget=1000,
        testing=False,
        processor=None,
    ):
        """
        Initializes a new Team object.

        Args:
            season (str): The season of the team.
            next_gameweek (int): The next gameweek of the team.
            df_next_game (pd.DataFrame, optional): The dataframe containing next gameweek data. Must be set if processor is not passed.
            initial_xi (Type["Team"], optional): The previous team for updates to be made to.
            odds_weight_def (float, optional): The weight for defensive player odds.
            odds_weight_fwd (float, optional): The weight for forward player odds.
            optimisation_obj (Type, optional): The selector object for picking the team.
            budget (int, optional): The budget available for the team.
            testing (bool, optional): Indicates if the team is being tested.
            processor (Any, optional): The processor object for preparing next gameweek data. Must be set if df_next_game is not passed.
        """

        self.season = season
        self.gameweek = next_gameweek
        self.processor = None
        self.odds_weight_def = odds_weight_def
        self.odds_weight_fwd = odds_weight_fwd

        self.df_next_game = df_next_game
        self.initial_xi = initial_xi  # Could be another object of the same class? That would be v. nice
        self.update_existing_team = len(initial_xi) > 0
        self.first_xi = pd.DataFrame()
        self.first_xv = pd.DataFrame()

        self.processor = processor
        self.budget = budget
        self.value = None

        self.selected = False
        self.testing = testing
        self.selector_obj = None
        self.selector = None

    def __repr__(self):
        return (
            f"Team object for GW {self.gameweek} of season {self.season}."
            f" Team selected: {self.selected}."
        )

    @property
    def selector_obj(self):
        if self._selector_obj is None and self.update_existing_team:
            self._selector_obj = UpdateXVSelector
        elif self._selector_obj is None and not self.update_existing_team:
            self._selector_obj = NewXVSelector
        return self._selector_obj

    @selector_obj.setter
    def selector_obj(self, val):
        self._selector_obj = val

    @property
    def selector(self):
        if self._selector is None:
            kwargs = {
                "player_df": self.df_next_game,
                "season": self.season,
                "budget": self.budget,
            }
            if self.update_existing_team:
                kwargs["initial_xi"] = self.initial_xi
            self._selector = self.selector_obj(**kwargs)
        return self._selector

    @selector.setter
    def selector(self, val):
        self._selector = val

    @property
    def df_next_game(self):
        """Collects next gameweek data from DB unless
        it was passed at instantiation"""
        if self._df_next_game.empty and self.processor is None:
            raise Exception("No processor has been set and no data has been passed.")
        elif self._df_next_game.empty:
            self._df_next_game = self.processor.prepare_next_gw()

        return self._df_next_game

    @df_next_game.setter
    def df_next_game(self, val):
        self._df_next_game = val

    @property
    def processer(self):
        return self._processor

    @processer.setter
    def processer(self, val):
        if self.df_next_game.empty and val is None:
            raise Exception("No processor has been set and no data has been passed.")
        self._processor = val

    @property
    def value(self):
        if self._value is None and not self.selected:
            raise Exception("Team has not been selected.")
        else:
            first_xi = self.first_xi[self.first_xi.picked == 1]
            value = int(first_xi.value.sum())
            self._value = value
        return self._value

    @value.setter
    def value(self, val):
        self._value = val

    def _pick_xv(self, max_changes):
        pass

    def pick_xi(self, max_changes=None):
        """Run picks"""
        if self.update_existing_team:
            assert max_changes is not None
        self.selector.pick_xv(max_changes)
        self.first_xi = self.selector.pick_xi()
        self.selected = True
        return self.first_xi
