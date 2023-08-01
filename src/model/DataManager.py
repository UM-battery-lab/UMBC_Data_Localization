from DataFetcher import DataFetcher
from DataIO import DataIO
from DataDeleter import DataDeleter
from DataFilter import DataFilter
from DataProcessor import DataProcessor
from logger_config import setup_logger



class DataManager:
    """
    The class to manage all the local data

    Attributes
    ----------
    dataIO: DataIO object
        The object to save and load data
    dataFetcher: DataFetcher object
        The object to fetch data from Voltaiq Studio
    dataDeleter: DataDeleter object
        The object to delete data
    dataFilter: DataFilter object
        The object to filter data
    logger: logger object
        The object to log information

    Methods
    -------
    update_test_data(trs=None, devs=None)
        Update the test data and directory structure
    filter_trs(device_id=None, device_name_substring=None, start_time=None)
        Filter the test records locally with the specified device id or name, and start time
    filter_dfs(device_id=None, device_name_substring=None, start_time=None)
        Filter the dataframes locally with the specified device id or name, and start time
    filter_trs_and_dfs(device_id=None, device_name_substring=None, start_time=None)
        Filter the test records and dataframes locally with the specified device id or name, and start time 
    """
    def __init__(self):
        self.dataIO = DataIO()
        self.dataFetcher = DataFetcher()
        self.dataDeleter = DataDeleter()
        self.dataFilter = DataFilter(self.dataIO)
        self.dataProcessor = DataProcessor(self.dataIO, self.dataFilter)
        self.logger = setup_logger()
        # self.__createdb()
    
    def __createdb(self):
        """
        Create the local database

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        # Fetch test records and devices
        trs = self.dataFetcher.fetch_trs()
        devs = self.dataFetcher.fetch_devs()
        
        if trs is None or devs is None:
            self.logger.error('Failed to fetch data')
            return
        
        # Create device folder dictionary
        device_id_to_name = self.dataIO.create_dev_dic(devs)
        # Fetch time series data from test records
        dfs = self.dataFetcher.get_dfs_from_trs(trs)
        # Save test data and update directory structure
        self.dataIO.save_test_data_update_dict(trs, dfs, device_id_to_name)

    def update_test_data(self, trs=None, devs=None):
        """
        Update the test data and directory structure

        Parameters
        ----------
        trs: list of TestRecord objects (optional)
            The list of test records to be saved
        devs: list of Device objects (optional)
            The list of devices to be saved
        
        Returns
        -------
        None
        """
        # Load existing directory structure
        dir_structure = self.dataIO.load_directory_structure()
        existing_uuids = self.dataIO.load_uuid(dir_structure)
        uuid_to_last_dp_timestamp = self.dataIO.load_uuid_to_last_dp_timestamp(dir_structure)
        uuid_to_tr_path_and_df_path = self.dataIO.load_uuid_to_tr_path_and_df_path(dir_structure)

        # Filter out existing test records
        new_trs = []

        # Process all the trs and devs if they are not provided
        if not trs:
            trs = self.dataFetcher.fetch_trs()
        if not devs:
            devs = self.dataFetcher.fetch_devs()

        for tr in trs:
            if tr.uuid not in existing_uuids:
                self.logger.info(f'New test record found: {tr.uuid}')
                new_trs.append(tr)
            else:
                last_dp_timestamp = uuid_to_last_dp_timestamp[tr.uuid]
                if last_dp_timestamp >= tr.last_dp_timestamp:   
                    self.logger.info(f'No new data found for test record {tr.uuid}') 
                    continue
                # Delete the old test data and update the directory structure
                old_tr_file, old_df_file = uuid_to_tr_path_and_df_path[tr.uuid]
                self.logger.info(f'Deleting old test data: {old_tr_file}, {old_df_file}')
                self.dataDeleter.delete_file(old_tr_file)
                self.dataDeleter.delete_file(old_df_file)
                new_trs.append(tr)

        if not new_trs:
            self.logger.info('No new test data found after filtering out existing records')
            return

        # TODO: We should check if the device folder exists before creating it
        # Now we just do check processing in create_dev_dic()   
        devices_id_to_name = self.dataIO.create_dev_dic(devs)

        # Get dataframes
        dfs = self.dataFetcher.get_dfs_from_trs(new_trs)
        # Save new test data and update directory structure
        self.dataIO.save_test_data_update_dict(new_trs, dfs, devices_id_to_name)

    def filter_trs(self, device_id=None, device_name_substring=None, start_time=None):
        """
        Filter the test records locally with the specified device id or name, and start time

        Parameters
        ----------
        device_id: str, optional
            The device id of the test record to be found
        device_name_substring: str, optional
            The substring of the device name of the test record to be found
        start_time: str, optional
            The start time of the test record to be found, in the format of 'YYYY-MM-DD_HH-MM-SS'
        
        Returns
        -------
        list of str
            The list of paths of the test records that match the specified device id or name, and start time
        """
        return self.dataFilter.filter_trs(device_id, device_name_substring, start_time)
    
    def filter_dfs(self, device_id=None, device_name_substring=None, start_time=None):
        """
        Filter the dataframes locally with the specified device id or name, and start time

        Parameters
        ----------
        device_id: str, optional
            The device id of the dataframe to be found
        device_name_substring: str, optional
            The substring of the device name of the dataframe to be found
        start_time: str, optional
            The start time of the dataframe to be found, in the format of 'YYYY-MM-DD_HH-MM-SS'
        
        Returns
        -------
        list of str
            The list of paths of the dataframes that match the specified device id or name, and start time
        """
        return self.dataFilter.filter_dfs(device_id, device_name_substring, start_time)
    

    def filter_trs_and_dfs(self, device_id=None, device_name_substring=None, start_time=None):
        """
        Filter the test records and dataframes locally with the specified device id or name, and start time

        Parameters
        ----------
        device_id: str, optional
            The device id of the dataframe to be found
        device_name_substring: str, optional
            The substring of the device name of the dataframe to be found
        start_time: str, optional
            The start time of the dataframe to be found, in the format of 'YYYY-MM-DD_HH-MM-SS'
        
        Returns
        -------
        list of str
            The list of paths of the test records that match the specified device id or name, and start time
        list of str
            The list of paths of the dataframes that match the specified device id or name, and start time
        """
        return self.dataFilter.filter_trs_and_dfs(device_id, device_name_substring, start_time)