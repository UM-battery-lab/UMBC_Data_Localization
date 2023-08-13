import logging

class SingletonLogger:
    _logger = None

    @classmethod
    def get_instance(cls):
        if not cls._logger:
            cls._logger = logging.getLogger(__name__)
            cls._logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            cls._logger.addHandler(handler)
        return cls._logger

def setup_logger():
    return SingletonLogger.get_instance()
