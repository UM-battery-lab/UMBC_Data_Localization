import shutil
import os
from concurrent.futures import ThreadPoolExecutor
from logger_config import setup_logger

from constants import ROOT_PATH, JSON_FILE_PATH
from save.file_io import load_dev_name, save_to_json, load_directory_structure
from concurrent.futures import ThreadPoolExecutor

# Setup logger
logger = setup_logger()

def delete_file(file_path):
    try:
        os.remove(file_path)
    except Exception as e:
        logger.error(f'Error while deleting file {file_path}: {e}')

def delete_test_data(devs):
    """"
    Delete the test data of the given devices

    Parameters
    ----------
    devs: list of Device objects
        The list of devices to be deleted

    Returns
    -------
    None
    """
    dir_structure = load_directory_structure()
    existing_dev_names = load_dev_name(dir_structure)

    for device in devs:
        if device.name in existing_dev_names:
            device_folder = os.path.join(ROOT_PATH, device.name)
            if os.path.exists(device_folder):
                with ThreadPoolExecutor() as executor:
                    for root, dirs, files in os.walk(device_folder):
                        for file in files:
                            if file.endswith('.pickle'):
                                file_path = os.path.join(root, file)
                                executor.submit(delete_file, file_path)
                # Delete the device folder after deleting all test data
                shutil.rmtree(device_folder)
            # Update directory structure
            dir_structure = [record for record in dir_structure if record['tr_name'] != device.name]            
        else:
            logger.info(f"No test data found for device {device.name}")
    save_to_json(dir_structure, JSON_FILE_PATH)