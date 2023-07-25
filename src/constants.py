import os
from datetime import timedelta, datetime, timezone

DATE_FORMAT = '%Y-%m-%d_%H-%M-%S'
ROOT_PATH = os.path.join(os.path.dirname(os.getcwd()), 'voltaiq_data')
JSON_FILE_PATH = os.path.join(ROOT_PATH, 'directory_structure.json')
TIME_TOLERANCE = timedelta(hours=2)


def UNIX_timestamp_to_datetime(timestamp, tzinfo=timezone(timedelta(days=-1, seconds=72000))):
    """
    Convert UNIX timestamp to datetime object

    Parameters
    ----------
    timestamp: int
        The UNIX timestamp in milliseconds
    tzinfo: timezone, optional
        The timezone of the datetime object
    
        
    Returns
    -------
    datetime
        The datetime object
    """
    timestamp_in_seconds = int(timestamp / 1000)
    return datetime.fromtimestamp(timestamp_in_seconds, tz=tzinfo)