from pandas import DataFrame
import pytest
import sys
import os
from team.select import NewXVSelector, UpdateXVSelector, XISelector


EXP_XV = sorted(
    [
        524,
        29,
        19,
        362,
        85,
        5,
        526,
        220,
        343,
        83,
        308,
        77,
        60,
        353,
        528,
    ]
)

EXP_XI = sorted([29, 19, 362, 85, 5, 526, 83, 308, 77, 60, 353])
INITIAL_XV = [5, 19, 20, 60, 85, 263, 294, 342, 353, 355, 362, 377, 409, 430, 509]
NEW_XV = [5, 19, 20, 60, 77, 85, 263, 294, 342, 353, 355, 362, 377, 430, 509]


## NewXVSelector
def test_NewXVSelector_instantiates(df_next_game: DataFrame):
    xv_selector = NewXVSelector(df_next_game, season=24)
    assert not xv_selector.player_df.empty


def test_NewXVSelector_fails_instantiates(df_next_game: DataFrame):
    with pytest.raises(TypeError):
        NewXVSelector(season=24)
    with pytest.raises(TypeError):
        NewXVSelector(df_next_game)


## Problem var
def test_NewXVSelector_creates_problem(df_next_game: DataFrame):
    xv_selector = NewXVSelector(df_next_game, season=24)
    prob = xv_selector.initialise_xv_prob()
    assert len(prob.constraints) == 1679
    assert prob.sol_status == 0


def test_NewXVSelector_pick_xv(df_next_game: DataFrame):
    xv_selector = NewXVSelector(df_next_game, season=24)
    xv_selector.pick_xv()
    assert len(xv_selector.first_xv) == 836

    first_xv = xv_selector.first_xv[xv_selector.first_xv.picked == 1]
    assert len(first_xv) == 15
    assert sorted(first_xv.element.to_list()) == EXP_XV


# XI Selector
def test_XISelector_instantiates(df_first_xv: DataFrame):
    xi_selector = XISelector(df_first_xv, season=24)
    assert not xi_selector.player_df.empty


def test_XI_Selector_creates_problem(df_first_xv: DataFrame):
    xi_selector = XISelector(df_first_xv, season=24)
    prob = xi_selector._initialise_xi_prob()
    assert len(prob.constraints) == 9
    assert prob.sol_status == 0


def test_XISelector_fails_instantiates(df_first_xv: DataFrame):
    with pytest.raises(TypeError):
        XISelector(season=24)
    with pytest.raises(TypeError):
        XISelector(df_first_xv)


def test_XISelector_pick_xi(df_first_xv: DataFrame):
    xi_selector = XISelector(df_first_xv, season=24)
    xi_selector.pick_xi()
    assert len(xi_selector.first_xi) == 836
    first_xi = xi_selector.first_xi[xi_selector.first_xi.first_xi == 1]
    assert len(first_xi) == 11
    assert sorted(first_xi.element.to_list()) == EXP_XI
    xv = sorted(
        xi_selector.first_xi[xi_selector.first_xi.picked == 1].element.to_list()
    )
    assert xv == EXP_XV


## Update XV Selector
def test_UpdateXVSelector_instantiates(df_next_game: DataFrame):
    xv_selector = UpdateXVSelector(df_next_game, season=24, initial_xi=INITIAL_XV)
    assert not xv_selector.player_df.empty
    assert len(xv_selector.initial_xi) == 15


def test_UpdateXVSelector_fails_instantiates(df_next_game: DataFrame):
    with pytest.raises(TypeError):
        UpdateXVSelector(season=24, initial_xi=INITIAL_XV)
    with pytest.raises(TypeError):
        UpdateXVSelector(df_next_game, initial_xi=INITIAL_XV)
    with pytest.raises(TypeError):
        UpdateXVSelector(df_next_game, season=24)


def test_UpdateXVSelector_adds_inital_team_col(df_next_game: DataFrame):
    xv_selector = UpdateXVSelector(df_next_game, season=24, initial_xi=INITIAL_XV)
    inital_xv = xv_selector.player_df[xv_selector.player_df["initial_xi"]]
    assert len(inital_xv) == 15


def test_UpdateXVSelector_creates_problem(df_next_game: DataFrame):
    xv_selector = UpdateXVSelector(df_next_game, season=24, initial_xi=INITIAL_XV)
    prob = xv_selector.initialise_xv_prob()
    assert len(prob.constraints) == 1680
    assert prob.sol_status == 0


def test_UpdateXVSelector_pick_xi(df_next_game: DataFrame):
    xv_selector = UpdateXVSelector(df_next_game, season=24, initial_xi=INITIAL_XV)
    xv_selector.pick_xv()
    assert len(xv_selector.first_xv) == 836
    first_xv = xv_selector.first_xv[xv_selector.first_xv.picked == 1]
    assert len(first_xv) == 15
    assert sorted(first_xv.element.to_list()) == NEW_XV
