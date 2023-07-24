from datetime import datetime

from constants import DATE_FORMAT, TIME_TOLERANCE, JSON_FILE_PATH
from save.file_io import load_directory_structure
from logger_config import setup_logger

# Setup logger
logger = setup_logger()


def find_records(device_id=None, device_name_substring=None, start_time=None):
    """
    Find the records with the specified device id or name, and start time

    Parameters
    ----------
    device_id: str, optional
        The device id of the record to be found
    device_name_substring: str, optional
        The substring of the device name of the record to be found
    start_time: str, optional
        The start time of the record to be found, in the format of 'YYYY-MM-DD_HH-MM-SS'
    
    Returns
    -------
    list of dict
        The list of records that match the specified device id or name, and start time
    """
    logger.info(f"Finding records with device_id={device_id}, device_name_substring={device_name_substring}, start_time={start_time}")
    
    dir_structure = load_directory_structure()
    
    if start_time is not None:
        start_time = datetime.strptime(start_time, DATE_FORMAT)
    
    matching_records = [
        record
        for record in dir_structure
        if (device_id is None or record['device_id'] == device_id) and
           (device_name_substring is None or device_name_substring in record['tr_name']) and
           (start_time is None or abs(datetime.strptime(record['start_time'], DATE_FORMAT) - start_time) <= TIME_TOLERANCE)
    ]
    logger.info(f"Found {len(matching_records)} matching records")
    return matching_records

def find_trs(device_id=None, device_name_substring=None, start_time=None):
    """
    Find the test records with the specified device id or name, and start time

    Parameters
    ----------
    device_id: str, optional
        The device id of the test record to be found
    device_name_substring: str, optional
        The substring of the device name of the test record to be found
    start_time: str, optional
        The start time of the test record to be found, in the format of 'YYYY-MM-DD_HH-MM-SS'
    
    Returns
    -------
    list of str
        The list of paths of the test records that match the specified device id or name, and start time
    """
    matching_records = find_records(device_id=device_id, device_name_substring=device_name_substring, start_time=start_time)
    return [record['tr_path'] for record in matching_records]

def find_dfs(device_id=None, device_name_substring=None, start_time=None):
    """
    Find the dataframes with the specified device id or name, and start time

    Parameters
    ----------
    device_id: str, optional
        The device id of the dataframe to be found
    device_name_substring: str, optional
        The substring of the device name of the dataframe to be found
    start_time: str, optional
        The start time of the dataframe to be found, in the format of 'YYYY-MM-DD_HH-MM-SS'
    
    Returns
    -------
    list of str
        The list of paths of the dataframes that match the specified device id or name, and start time
    """
    matching_records = find_records(device_id=device_id, device_name_substring=device_name_substring, start_time=start_time)
    return [record['df_path'] for record in matching_records]

def find_trs_and_dfs(device_id=None, device_name_substring=None, start_time=None):
    """
    Find the test records and dataframes with the specified device id or name, and start time

    Parameters
    ----------
    device_id: str, optional
        The device id of the dataframe to be found
    device_name_substring: str, optional
        The substring of the device name of the dataframe to be found
    start_time: str, optional
        The start time of the dataframe to be found, in the format of 'YYYY-MM-DD_HH-MM-SS'
    
    Returns
    -------
    list of str
        The list of paths of the test records that match the specified device id or name, and start time
    list of str
        The list of paths of the dataframes that match the specified device id or name, and start time
    """
    matching_records = find_records(device_id=device_id, device_name_substring=device_name_substring, start_time=start_time)
    return [record['tr_path'] for record in matching_records], [record['df_path'] for record in matching_records]