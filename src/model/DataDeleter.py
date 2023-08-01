import os
from logger_config import setup_logger


class DataDeleter:
    def __init__(self):
        self.logger = setup_logger()

    def delete_file(self, file_path):
        try:
            os.remove(file_path)
        except Exception as e:
            self.logger.error(f'Error while deleting file {file_path}: {e}')