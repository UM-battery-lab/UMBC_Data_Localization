import logging
from config import logger_config

class SingletonLogger:
    """
    The singleton logger class

    Methods
    -------
    get_instance()
        Get the singleton logger instance
    """
    _logger = None
    @classmethod
    def get_instance(cls):
        if not cls._logger:
            cls._logger = logging.getLogger(__name__)
            cls._logger.setLevel(logger_config.LOG_LEVEL)

            # create console handler
            handler = logging.StreamHandler()
            formatter = logging.Formatter(logger_config.LOG_FORMAT)
            handler.setFormatter(formatter)
            cls._logger.addHandler(handler)

            # create file handler
            file_handler = logging.FileHandler(logger_config.LOG_FILE_PATH)
            file_handler.setFormatter(formatter)
            cls._logger.addHandler(file_handler)

        return cls._logger

def setup_logger():
    return SingletonLogger.get_instance()
