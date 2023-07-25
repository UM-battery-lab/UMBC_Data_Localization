import pandas as pd
# Enviroment Variables:
from dotenv import load_dotenv
load_dotenv('voltaiq_mac.env')
import voltaiq_studio as vs

from logger_config import setup_logger
# Setup logger
logger = setup_logger()

def fetch_trs():
    """
    Fetch data from Voltaiq Studio

    Returns
    -------
    list of TestRecord objects
        The list of test records
    """
    try:
        trs = vs.get_test_records()
        logger.info(f"Fetched {len(trs)} test records.")
        return trs
    except Exception as e:
        logger.error(f"Failed to fetch trs: {e}")
        return None
    
def fetch_devs():
    """
    Fetch data from Voltaiq Studio

    Returns
    -------
    list of Device objects
        The list of devices
    """
    try:
        devs = vs.get_devices()
        logger.info(f"Fetched {len(devs)} devices.")
        return devs
    except Exception as e:
        logger.error(f"Failed to fetch devs: {e}")
        return None
    

def get_data_from_test_record(tr):
    """
    Get data from a TestRecord object

    Parameters
    ----------
    tr: TestRecord object
        The test record to be processed

    Returns
    -------
    DataFrame
        The processed data
    """
    logger.info(f"Getting data for TestRecord with ID: {tr.id}")
    try:
        reader = tr.make_time_series_reader()
        trace_keys = tr.trace_keys
        reader.add_trace_keys(*trace_keys)
        reader.add_info_keys('i_cycle_num')
        df = pd.DataFrame()
        for batch in reader.read_pandas_batches(): # Generator to read pandas data frames in supported sizes
            df = pd.concat([df,batch])
    except Exception as e:
        logger.error(f"Failed to get data for TestRecord with ID: {tr.id}. Error: {e}")
        return None

    logger.info(f"Successfully got data for TestRecord with ID: {tr.id}")
    return df

def get_dfs_from_trs(trs):
    """
    Get data from a list of TestRecord objects

    Parameters
    ----------
    trs: list of TestRecord objects
        The list of test records to be processed
    
    Returns
    -------
    list of DataFrame
    """
    dfs = [get_data_from_test_record(tr) for tr in trs]
    return dfs