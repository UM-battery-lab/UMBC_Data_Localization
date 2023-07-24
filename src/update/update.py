from save.file_io import load_uuid, load_directory_structure
from save.save_data import create_dev_dic, save_test_data_update_dict
from logger_config import setup_logger

# Setup logger
logger = setup_logger()

def update_test_data(trs, devs):
    """
    Update the test data and directory structure

    Parameters
    ----------
    trs: list of TestRecord objects
        The list of test records to be saved
    devs: list of Device objects
        The list of devices to be saved
    
    Returns
    -------
    None
    """
    # Load existing directory structure
    dir_structure = load_directory_structure()
    existing_uuids = load_uuid(dir_structure)

    # Filter out existing test records
    # TODO: Potential problem: if the old test record is updated, we should still save it
    new_trs = [tr for tr in trs if tr.uuid not in existing_uuids]

    if not new_trs:
        logger.info('No new test data found after filtering out existing records')
        return

    # TODO: We should check if the device folder exists before creating it
    # Now we just do check processing in create_dev_dic()   
    device_paths = create_dev_dic(devs)

    # Save new test data and update directory structure
    save_test_data_update_dict(new_trs, device_paths)
