import json
import random

import numpy as np
import pandas as pd
import pytest

from lionel.selector.fpl.xi_selector import XISelector
from lionel.selector.fpl.xv_selector import XVSelector
from lionel.utils import setup_logger

logger = setup_logger(__name__)


@pytest.fixture
def candidates_xv_df():
    with open("tests/data/candidates_xv.json", "r") as f:
        data = json.load(f)
    return pd.DataFrame(data)


@pytest.fixture
def candidates_xi_df():
    with open("tests/data/candidates_xi.json", "r") as f:
        data = json.load(f)
    return pd.DataFrame(data)


def test_xv_selector(candidates_xv_df, candidates_xi_df):
    """Make sure that it makes consistent selections"""

    random.seed(35)
    xv = XVSelector(candidates_xv_df)
    xv.select()
    xv_df = xv.candidate_df
    selections = xv_df[xv_df["xv"] == 1].player.tolist()
    assert sorted(selections) == [
        "player_1",
        "player_19",
        "player_24",
        "player_28",
        "player_30",
        "player_45",
        "player_46",
        "player_50",
        "player_53",
        "player_67",
        "player_68",
        "player_73",
        "player_79",
        "player_91",
        "player_98",
    ]

    assert sorted(candidates_xi_df.player.tolist()) == [
        "player_1",
        "player_19",
        "player_24",
        "player_28",
        "player_30",
        "player_45",
        "player_46",
        "player_50",
        "player_53",
        "player_67",
        "player_68",
        "player_73",
        "player_79",
        "player_91",
        "player_98",
    ]


def test_xi_selector(candidates_xi_df):
    random.seed(random.seed(4))
    np.random.seed(4)
    xi = XISelector(candidates_xi_df)
    xi.select()
    xi_df = xi.candidate_df
    selections = xi_df[xi_df["xi"] == 1].player.tolist()
    assert sorted(selections) == [
        "player_1",
        "player_19",
        "player_24",
        "player_28",
        "player_45",
        "player_46",
        "player_50",
        "player_53",
        "player_68",
        "player_73",
        "player_79",
    ]


def test_xv_then_xi(candidates_xv_df):
    random.seed(35)
    xv = XVSelector(candidates_xv_df)
    xv.select()
    xv_df = xv.candidate_df

    candidates_xi_df = xv_df[xv_df["xv"] == 1]

    selections = candidates_xi_df.player.tolist()
    assert sorted(selections) == [
        "player_1",
        "player_19",
        "player_24",
        "player_28",
        "player_30",
        "player_45",
        "player_46",
        "player_50",
        "player_53",
        "player_67",
        "player_68",
        "player_73",
        "player_79",
        "player_91",
        "player_98",
    ]

    random.seed(4)
    np.random.seed(4)
    xi = XISelector(candidates_xi_df)
    xi.select()

    xi_df = xi.candidate_df
    selections = xi_df[xi_df["xi"] == 1].player.tolist()
    assert sorted(selections) == [
        "player_1",
        "player_19",
        "player_24",
        "player_28",
        "player_45",
        "player_46",
        "player_50",
        "player_53",
        "player_68",
        "player_73",
        "player_79",
    ]
