import logging
from meerkat_abacus.config import config

handler = logging.StreamHandler()
formatter = logging.Formatter(config.LOGGING_FORMAT)
handler.setFormatter(formatter)
level = logging.getLevelName(config.LOGGING_LEVEL)

logger = logging.getLogger(config.LOGGER_NAME)
logger.setLevel(level)
logger.addHandler(handler)
logger.propagate = 0

logger.debug("Config initialised.")
