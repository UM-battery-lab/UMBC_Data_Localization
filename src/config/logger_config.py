import logging
import datetime
LOG_LEVEL = logging.INFO
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
today_str = datetime.date.today().strftime('%Y-%m-%d')
LOG_FILE_PATH = f'logs/app_{today_str}.log'

