import logging
import sys

_logger_instance = None


def create_logger_singleton(name="rotary", log_level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(log_level)
    logger.addHandler(stream_handler)

    return logger


def get_logger_instance():
    global _logger_instance

    if _logger_instance is None:
        _logger_instance = create_logger_singleton()

    return _logger_instance
