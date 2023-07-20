import os
import pickle
import json
from logger_config import setup_logger
from constants import ROOT_PATH, JSON_FILE_PATH, DATE_FORMAT

# Setup logger
logger = setup_logger()

def create_directory(directory_path):
    try:
        os.makedirs(directory_path, exist_ok=True)
    except Exception as err:
        logger.error(f'Error occurred while creating directory {directory_path}: {err}')

def save_to_pickle(data, file_path):
    try:
        with open(file_path, 'wb') as f:
            pickle.dump(data, f)
    except Exception as err:
        logger.error(f'Error occurred while writing file {file_path}: {err}')

def save_to_json(data, file_path):
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f)
    except Exception as err:
        logger.error(f'Error occurred while writing file {file_path}: {err}')

def load_directory_structure():
    """
    Load the directory structure file

    Returns
    -------
    list of dict
        The list of directory structure information
    """ 
    with open(JSON_FILE_PATH, 'r') as f:
            dir_structure = json.load(f)
    return dir_structure

def create_dev_dic(devs):
    """
    Create a dictionary of device id and device folder path

    Parameters
    ----------
    devs: list of Device objects
        The list of devices to be saved

    Returns
    -------
    dict
        The dictionary of device id and device folder path
    """
    device_paths = {}
    for dev in devs:
        device_folder = os.path.join(ROOT_PATH, dev.name)   
        create_directory(device_folder)
        device_paths[dev.id] = device_folder
    return device_paths


def load_uuid(dir_structure):
    """
    Load the uuids from the directory structure file

    Parameters
    ----------
    dir_structure: list of dict

    Returns
    -------
    set of str
        The set of uuids
    """ 
    return {record['uuid'] for record in dir_structure}

def load_dev_name(dir_structure):
    """
    Load the device names from the directory structure file

    Parameters
    ----------
    dir_structure: list of dict

    Returns
    -------
    set of str
        The set of device names
    """ 
    return {record['tr_name'] for record in dir_structure}

def save_test_data_update_dict(trs, device_paths):
    """
    Save test data to local disk and update the directory structure information
    
    Parameters
    ----------
    trs: list of TestRecord objects
        The list of test records to be saved
    device_paths: dict
        The dictionary of device id and device folder path

    Returns
    -------
    None
    """
    dir_structure = []
    # Load existing directory structure if it exists
    if os.path.exists(JSON_FILE_PATH):
        dir_structure = load_directory_structure()

    for tr in trs:
        device_folder = device_paths.get(tr.device_id)
        if device_folder is None:
            logger.error(f'Device folder not found for device id {tr.device_id}')
            continue
        start_time_str = tr.start_time.strftime(DATE_FORMAT)

        test_folder = os.path.join(device_folder, start_time_str)
        if not os.path.exists(test_folder):
            os.makedirs(test_folder)
        
        # Save the test data to a pickle file
        tr_path = os.path.join(test_folder, 'tr.pickle')
        save_to_pickle(tr, tr_path)


        # TODO: This part should be done in fetch/fetch.py and pass df as a parameter. 
        # Because make_time_series_reader() seems to use API call to get the data from Voltaiq
        reader = tr.make_time_series_reader()
        # TODO: Add the keys we want to save
        reader.add_trace_keys('h_current', 'h_potential')
        reader.add_info_keys('i_cycle_num')
        df = reader.read_pandas()

        # Save the time series data to a pickle file
        df_path = os.path.join(test_folder, 'df.pickle')
        save_to_pickle(df, df_path)

        # Append the directory structure information to the list
        dir_structure.append({
            'uuid': tr.uuid,
            'device_id': tr.device_id,
            'tr_name': tr.name,  
            'start_time': start_time_str,
            'tr_path': tr_path,
            'df_path': df_path
        })

    # Save the directory structure information to a JSON file
    save_to_json(dir_structure, JSON_FILE_PATH)