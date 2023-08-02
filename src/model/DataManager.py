from DirStructure import DirStructure
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
        The object to filter data from the local disk
    dataProcessor: DataProcessor object
        The object to process data
    dirStructure: DirStructure object
        The object to manage the directory structure for the local data
    logger: logger object
        The object to log information
    
    Methods
    -------
    __createdb()
        Create the local database
    __updatedb()
        Update the local database
    update_test_data(trs=None, devs=None)
        Update the test data and directory structure with the specified test records and devices
    filter_trs(device_id=None, device_name_substring=None, start_time=None, tags=None)
        Filter the test records locally with the specified device id or name or start time or tags
    filter_dfs(device_id=None, device_name_substring=None, start_time=None, tags=None)
        Filter the dataframes locally with the specified device id or name or start time or tags
    filter_trs_and_dfs(device_id=None, device_name_substring=None, start_time=None, tags=None)
        Filter the test records and dataframes locally with the specified device id or name or start time or tags
    """
    def __init__(self):
        self.dirStructure = DirStructure()
        self.dataIO = DataIO(self.dirStructure)
        self.dataFetcher = DataFetcher()
        self.dataDeleter = DataDeleter()
        self.dataFilter = DataFilter(self.dataIO, self.dirStructure)
        self.dataProcessor = DataProcessor(self.dataIO, self.dataFilter, self.dirStructure)
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

    def __updatedb(self):
        """
        Update the local database

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
        self.update_test_data(trs, devs)

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
        existing_uuids = self.dirStructure.load_uuid()
        uuid_to_last_dp_timestamp = self.dirStructure.load_uuid_to_last_dp_timestamp()
        uuid_to_tr_path_and_df_path = self.dirStructure.load_uuid_to_tr_path_and_df_path()

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

        devices_id_to_name = self.dataIO.create_dev_dic(devs)

        # Get dataframes
        dfs = self.dataFetcher.get_dfs_from_trs(new_trs)
        # Save new test data and update directory structure
        self.dataIO.save_test_data_update_dict(new_trs, dfs, devices_id_to_name)

    def filter_trs(self, device_id=None, device_name_substring=None, start_time=None, tags=None):
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
        tags: list of str, optional
            The list of tags of the test record to be found
        
        Returns
        -------
        list of test records
            The list of test records that match the specified device id or name, and start time and tags
        
        """
        tr_paths = self.dataFilter.filter_trs(device_id, device_name_substring, start_time, tags)
        return self.dataIO.load_trs(tr_paths)
    
    def filter_dfs(self, device_id=None, device_name_substring=None, start_time=None, tags=None):
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
        list of dataframes
            The list of dataframes that match the specified device id or name, and start time and tags
        """
        df_paths = self.dataFilter.filter_dfs(device_id, device_name_substring, start_time, tags)
        return self.dataIO.load_dfs(df_paths)
    

    def filter_trs_and_dfs(self, device_id=None, device_name_substring=None, start_time=None, tags=None):
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
        list of test records
            The list of test records that match the specified device id or name, and start time and tags
        list of dataframes
            The list of dataframes that match the specified device id or name, and start time and tags
        """
        tr_paths, df_paths = self.dataFilter.filter_trs_and_dfs(device_id, device_name_substring, start_time, tags)
        return self.dataIO.load_trs(tr_paths), self.dataIO.load_dfs(df_paths)