import requests
import logging
import pandas as pd


def setup_logger(name):
    logging.basicConfig()
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    return logger


logger = setup_logger(__name__)


def get_response(url):
    r = requests.get(url)
    try:
        assert r.ok
    except AssertionError as e:
        logger.exception("Failed url: " + str(url))
        raise e
    return r.json()


def get_session(url):
    pass


def undo_dummies(df, prefix, default):
    cols = [col for col in df.columns if col.startswith(prefix)]
    df[prefix] = pd.from_dummies(df[cols], default_category=default)
    df[prefix] = df[prefix].str.split("_").str[-1]
    df = df.drop(columns=cols)
    return df
