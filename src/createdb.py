from save.save_data import create_dev_dic, save_test_data_update_dict
from fetch.fetch_data import fetch_trs, fetch_devs, get_dfs_from_trs
from logger_config import setup_logger

# Setup logger
logger = setup_logger()

def create_test_data():
    """
    Create the test data and directory structure

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    # Fetch test records and devices
    trs = fetch_trs()
    devs = fetch_devs()

    test_trs = trs[:30]
    
    if trs is None or devs is None:
        logger.error('Failed to fetch data')
        return

    # Create device folder dictionary
    device_paths = create_dev_dic(devs)

    # Fetch time series data from test records
    dfs = get_dfs_from_trs(test_trs)

    # Save test data and update directory structure
    save_test_data_update_dict(test_trs, dfs, device_paths)

if __name__ == '__main__':
    create_test_data()
