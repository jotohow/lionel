import requests
import logging


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
