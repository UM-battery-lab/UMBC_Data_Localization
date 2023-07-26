import os
import pickle
import json
from logger_config import setup_logger
from constants import JSON_FILE_PATH

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
    return {record['dev_name'] for record in dir_structure}

def load_uuid_to_last_dp_timestamp(dir_structure):
    return {record['uuid']: record['last_dp_timestamp'] for record in dir_structure}

def load_uuid_to_tr_path_and_df_path(dir_structure):
    return {record['uuid']: (record['tr_path'], record['df_path']) for record in dir_structure}

def load_record(file_path):
    # Load the test record from the pickle file
    with open(file_path, "rb") as f:
        record = pickle.load(f)
    logger.info(f"Loaded test record from {file_path}")
    logger.info(f"Record: {record}")
    return record
