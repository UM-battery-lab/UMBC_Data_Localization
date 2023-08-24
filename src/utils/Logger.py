import logging
import os
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
    def _ensure_log_file_directory_exists(cls):
        """
        Ensure the directory for the log file exists.
        If not, create it.
        """
        log_dir = os.path.dirname(logger_config.LOG_FILE_PATH)
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
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
            cls._ensure_log_file_directory_exists()
            file_handler = logging.FileHandler(logger_config.LOG_FILE_PATH)
            file_handler.setFormatter(formatter)
            cls._logger.addHandler(file_handler)

        return cls._logger

def setup_logger():
    return SingletonLogger.get_instance()
