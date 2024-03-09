import pytest
import os
from pathlib import Path
import pandas as pd
import json
import sys

CWD = Path(os.path.dirname(os.path.realpath(__file__)))
SRC = CWD.parent / "src"
sys.path.append(str(SRC))
DATA_DIR = CWD / "data"

from utils import setup_logger  # noqa: E402

logger = setup_logger(__name__)


@pytest.fixture
def df_next_game():
    logger.info("Loading df_next_game")
    return pd.read_csv(DATA_DIR / "df_next_game.csv")


@pytest.fixture
def picks():
    with open(DATA_DIR / "my_team.json") as f:
        picks = json.load(f)["picks"]
    return picks
