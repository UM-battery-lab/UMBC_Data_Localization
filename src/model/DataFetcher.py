import pandas as pd
import time
# Enviroment Variables:
from dotenv import load_dotenv
load_dotenv('voltaiq_mac.env')
import voltaiq_studio as vs

from src.utils.Logger import setup_logger


class DataFetcher:
    """
    The class to fetch data from Voltaiq Studio

    Attributes
    ----------
    logger: logger object
        The logger object
    vs: Voltaiq Studio object
        The Voltaiq Studio object

    Methods
    -------
    fetch_trs()
        Fetch all the TestRecords data from Voltaiq Studio
    fetch_devs()
        Fetch all the Devices data from Voltaiq Studio
    get_df_from_tr(tr, trace_keys=None)
        Get Dataframe from Voltaiq Studio based on a TestRecord object
    get_dfs_from_trs(trs, trace_key=None)
        Get Dataframes from Voltaiq Studio based on a list of TestRecord objects
    get_dev_from_tr(tr)
        Get Device from Voltaiq Studio based on a TestRecord object
    get_devs_from_trs(trs)
        Get Devices from Voltaiq Studio based on a list of TestRecord objects
    """
    def __init__(self):
        # Setup logger
        self.logger = setup_logger()
        self.vs = vs

    def fetch_trs(self):
        """
        Fetch data from Voltaiq Studio

        Returns
        -------
        list of TestRecord objects
            The list of test records
        """
        try:
            trs = self.vs.get_test_records()
            self.logger.info(f"Fetched {len(trs)} test records.")
            return trs
        except Exception as e:
            self.logger.error(f"Failed to fetch trs: {e}")
            return None
        
    def fetch_devs(self):
        """
        Fetch data from Voltaiq Studio

        Returns
        -------
        list of Device objects
            The list of devices
        """
        try:
            devs = self.vs.get_devices()
            self.logger.info(f"Fetched {len(devs)} devices.")
            return devs
        except Exception as e:
            self.logger.error(f"Failed to fetch devs: {e}")
            return None
    
    def get_df_from_tr(self, tr, trace_keys=None):
        """
        Get data from a TestRecord object

        Parameters
        ----------
        tr: TestRecord object
            The test record to be processed
        trace_keys: list of str, optional
            The trace keys for the data to be get

        Returns
        -------
        DataFrame
            The processed data
        """
        self.logger.info(f"Getting DataFrame for TestRecord with ID: {tr.id}")
        try:
            reader = tr.make_time_series_reader()
            if trace_keys is None:
                trace_keys = tr.trace_keys
                self.logger.info(f"Trace Keys: {trace_keys}")

            reader.add_trace_keys(*trace_keys)

            reader.add_info_keys('i_cycle_num')
            # df = pd.DataFrame()
            # for batch in reader.read_pandas_batches(): # Generator to read pandas data frames in supported sizes
            #     df = pd.concat([df,batch])
            df = reader.read_pandas()
            time.sleep(2)
        except Exception as e:
            self.logger.error(f"Failed to get DataFrame for TestRecord with ID: {tr.id}. Error: {e}")
            return None
        self.logger.info(f"Successfully got DataFrame for TestRecord with ID: {tr.id}")
        return df
    
    def get_dfs_from_trs(self, trs, trace_key=None):
        """
        Get data from a list of TestRecord objects

        Parameters
        ----------
        trs: list of TestRecord objects
            The list of test records to be processed
        trace_key: str, optional
            The trace key for the data to be get
        
        Returns
        -------
        list of DataFrame
        """
        dfs = [self.get_df_from_tr(tr, trace_key) for tr in trs]
        return dfs
    
    def get_dev_from_tr(self, tr):
        """
        Get device from a TestRecord object

        Parameters
        ----------
        tr: TestRecord object
            The test record to be processed
        
        Returns
        -------
        Device object
            The device object
        """
        device_id = tr.device_id
        return self.vs.get_device(device_id)

    def get_devs_from_trs(self, trs):
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
        devs = [self.get_dev_from_tr(tr) for tr in trs]
        return devs
    
    def get_cycle_stats(self, tr):
        """
        Get cycle stats from a TestRecord object

        Parameters
        ----------
        tr: TestRecord object
            The test record to be processed
        
        Returns
        -------
        dict
            The cycle stats
        """
        self.logger.info(f"Getting cycle stats for TestRecord with ID: {tr.id}")
        try:
            cycle_stats = tr.get_cycle_stats()
        except Exception as e:
            self.logger.error(f"Failed to get cycle stats for TestRecord with ID: {tr.id}. Error: {e}")
            return None
        self.logger.info(f"Successfully got cycle stats for TestRecord with ID: {tr.id}")
        return cycle_stats
    
    def get_cycle_stats_from_trs(self, trs):
        """
        Get cycle stats from a list of TestRecord objects

        Parameters
        ----------
        trs: list of TestRecord objects
            The list of test records to be processed
        
        Returns
        -------
        list of dict
            The cycle stats
        """
        cycle_stats = [self.get_cycle_stats(tr) for tr in trs]
        return cycle_stats  