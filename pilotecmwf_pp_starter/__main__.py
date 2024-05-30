import logging
import time

_LOGGER = logging.getLogger(__name__)


def simulate_job():
    _LOGGER.info("Here we are. Now going to sleep.")
    time.sleep(5)
    _LOGGER.info("Sleeping finished. Back to work.")


simulate_job()
