from save.file_io import load_uuid, load_directory_structure, load_uuid_to_last_dp_timestamp, load_uuid_to_tr_path_and_df_path
from save.save_data import create_dev_dic, save_test_data_update_dict
from delete.delete import delete_file
from fetch.fetch_data import get_dfs_from_trs
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
    uuid_to_last_dp_timestamp = load_uuid_to_last_dp_timestamp(dir_structure)
    uuid_to_tr_path_and_df_path = load_uuid_to_tr_path_and_df_path(dir_structure)

    # Filter out existing test records
    new_trs = []

    for tr in trs:
        if tr.uuid not in existing_uuids:
            logger.info(f'New test record found: {tr.uuid}')
            new_trs.append(tr)
        else:
            last_dp_timestamp = uuid_to_last_dp_timestamp[tr.uuid]
            if last_dp_timestamp >= tr.last_dp_timestamp:   
                logger.info(f'No new data found for test record {tr.uuid}') 
                continue
            # Delete the old test data and update the directory structure
            old_tr_file, old_df_file = uuid_to_tr_path_and_df_path[tr.uuid]
            logger.info(f'Deleting old test data: {old_tr_file}, {old_df_file}')
            delete_file(old_tr_file)
            delete_file(old_df_file)
            new_trs.append(tr)

    if not new_trs:
        logger.info('No new test data found after filtering out existing records')
        return

    # TODO: We should check if the device folder exists before creating it
    # Now we just do check processing in create_dev_dic()   
    devices_id_to_name = create_dev_dic(devs)

    # Get dataframes
    dfs = get_dfs_from_trs(new_trs)
    # Save new test data and update directory structure
    save_test_data_update_dict(new_trs, dfs, devices_id_to_name)
