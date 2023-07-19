import json
from datetime import datetime

from constants import DATE_FORMAT, TIME_TOLERANCE, JSON_FILE_PATH


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
    list of str
        The list of paths of the records that match the specified device id or name, and start time
    list of str
        The list of paths of the dataframes that match the specified device id or name, and start time
    """
    # Load the directory structure file
    with open('directory_structure.json', 'r') as f:
        dir_structure = json.load(f)

    if start_time is not None:
        # Convert start_time to a datetime object for comparison
        start_time = datetime.strptime(start_time, DATE_FORMAT)
    
    matching_trs = []
    matching_dfs = []

    # Find the records with the specified device id or name, and start time
    for record in dir_structure:
        if (device_id is not None and record['device_id'] == device_id) or \
           (device_name_substring is not None and device_name_substring in record['tr_name']):
            if start_time is not None:
                record_start_time = datetime.strptime(record['start_time'], DATE_FORMAT)
                # Check if the record's start time is within +-2 hours of the specified start time
                if abs(record_start_time - start_time) <= TIME_TOLERANCE:
                    matching_trs.append(record['tr_path'])
                    matching_dfs.append(record['df_path'])
            else:
                matching_trs.append(record['record_path'])
                matching_dfs.append(record['df_path'])

    return matching_trs, matching_dfs