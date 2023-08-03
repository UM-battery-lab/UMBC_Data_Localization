import os
from datetime import timedelta

DATE_FORMAT = '%Y-%m-%d_%H-%M-%S'
ROOT_PATH = os.path.join(os.path.dirname(os.getcwd()), 'voltaiq_data')
JSON_FILE_PATH = os.path.join(ROOT_PATH, 'directory_structure.json')
TIME_TOLERANCE = timedelta(hours=2)