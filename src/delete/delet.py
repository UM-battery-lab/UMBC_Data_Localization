import shutil
import os

from constants import ROOT_PATH

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
    for device in devs:
        device_folder = os.path.join(ROOT_PATH, device.name)

    if os.path.exists(device_folder):
        for root, dirs, files in os.walk(device_folder):
            for file in files:
                if file.endswith('.pickle'):
                    os.remove(os.path.join(root, file))

def delete_folders(devs):
    """"
    Delete the folders of the given devices
    
    Parameters
    ----------
    devs: list of Device objects
        The list of devices to be deleted

    Returns
    -------
    None
    """
    for device in devs:
        device_folder = os.path.join(ROOT_PATH, device.name)

        if os.path.exists(device_folder):
            shutil.rmtree(device_folder)