"""
Services providing core functionality.
"""

import logging

from pilotecmwf_pp_starter import CONFIG
from pilotecmwf_pp_starter.domain.greeting import Greeting


logger = logging.getLogger(__name__)


def get_greeting(name: str) -> Greeting:
    """
    Get personalized greeting

    :param name: name, as the name implies
    :type name: str

    :return: personalized greeting
    :rtype: Greeting
    """
    logger.debug('Personalizing greeting for %s...', name)

    return Greeting(message=f'Hello, {name} from {CONFIG.main.app_name}!')
