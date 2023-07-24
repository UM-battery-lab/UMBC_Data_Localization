import os

from save.file_io import create_directory, save_to_pickle, save_to_json, load_directory_structure
from constants import ROOT_PATH, JSON_FILE_PATH, DATE_FORMAT
from logger_config import setup_logger

# Setup logger
logger = setup_logger()

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

def save_test_data_update_dict(trs, dfs, device_paths):
    """
    Save test data to local disk and update the directory structure information
    
    Parameters
    ----------
    trs: list of TestRecord objects
        The list of test records to be saved
    dfs: list of pandas dataframe
        The list of dataframes to be saved
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

    for tr, df in zip(trs, dfs):
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