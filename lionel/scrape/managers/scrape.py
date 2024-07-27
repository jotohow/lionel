from utils import get_response, setup_logger
from dotenv import load_dotenv
import requests
import os
import pandas as pd

load_dotenv()

logger = setup_logger(__name__)
GW_URL = "https://fantasy.premierleague.com/api/entry/{team_id}/event/{gw}/picks/"


def get_entry_response(team_id, gw) -> dict:
    url = GW_URL.format(team_id=team_id, gw=gw)
    return get_response(url)


def _get_existing_player_elements(json_) -> list:
    picks = json_["picks"]
    assert len(picks) == 15, logger.exception(f"Expected 15 picks, got {len(picks)}")
    return [pick["element"] for pick in picks]


def get_existing_player_elements(team_id, gw) -> list:
    r = get_entry_response(team_id, gw)
    return _get_existing_player_elements(r)


def get_budget(json_) -> dict:
    value = json_["entry_history"]["value"]
    bank = json_["entry_history"]["bank"]
    return {"value": value, "bank": bank}


def get_my_team_info() -> dict:
    login = os.environ.get("FPL_LOGIN")
    pw = os.environ.get("FPL_PASSWORD")
    team_id = os.environ.get("FPL_TEAM_ID")

    if not all([login, pw, team_id]):
        logger.warning("Missing login credentials for FPL API.")
        raise ValueError("Missing login credentials")

    session = requests.session()
    headers = {
        "User-Agent": "Dalvik/2.1.0 (Linux; U; Android 5.1; PRO 5 Build/LMY47D)",
        "accept-language": "en",
    }
    payload = {
        "password": pw,
        "login": login,
        "redirect_uri": "https://fantasy.premierleague.com/a/login",
        "app": "plfpl-web",
    }
    url = "https://users.premierleague.com/accounts/login/"
    session.post(url, data=payload, headers=headers)
    response = session.get(f"https://fantasy.premierleague.com/api/my-team/{team_id}")
    try:
        assert response.ok
        return response.json()
    except Exception as e:
        logger.exception(f"Failed to get team info. Response: {response.status_code}")
        raise e


def get_sale_prices(json_) -> pd.DataFrame:
    keep_cols = ["element", "selling_price", "purchase_price"]
    return pd.DataFrame.from_dict(json_["picks"])[keep_cols]


def get_free_transfers(json_) -> int:
    return json_["transfers"]["limit"]
