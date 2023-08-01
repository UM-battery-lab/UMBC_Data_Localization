from DataFetcher import DataFetcher
from DataIO import DataIO
from DataDeleter import DataDeleter
from logger_config import setup_logger



class DataManager:
    """
    The class to manage all the local data

    Attributes
    ----------
    data_io: DataIO object
        The object to save and load data
    data_fetcher: DataFetcher object
        The object to fetch data from Voltaiq Studio
    data_deleter: DataDeleter object
        The object to delete data
    logger: logger object
        The object to log information

    Methods
    -------
    update_test_data(trs=None, devs=None)
        Update the test data and directory structure
    """
    def __init__(self):
        self.data_io = DataIO()
        self.data_fetcher = DataFetcher()
        self.data_deleter = DataDeleter()
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
        trs = self.data_fetcher.fetch_trs()
        devs = self.data_fetcher.fetch_devs()
        
        if trs is None or devs is None:
            self.logger.error('Failed to fetch data')
            return
        
        # Create device folder dictionary
        device_id_to_name = self.data_io.create_dev_dic(devs)
        # Fetch time series data from test records
        dfs = self.data_fetcher.get_dfs_from_trs(trs)
        # Save test data and update directory structure
        self.data_io.save_test_data_update_dict(trs, dfs, device_id_to_name)

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
        dir_structure = self.data_io.load_directory_structure()
        existing_uuids = self.data_io.load_uuid(dir_structure)
        uuid_to_last_dp_timestamp = self.data_io.load_uuid_to_last_dp_timestamp(dir_structure)
        uuid_to_tr_path_and_df_path = self.data_io.load_uuid_to_tr_path_and_df_path(dir_structure)

        # Filter out existing test records
        new_trs = []

        # Process all the trs and devs if they are not provided
        if not trs:
            trs = self.data_fetcher.fetch_trs()
        if not devs:
            devs = self.data_fetcher.fetch_devs()

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
                #TODO: Add delete_file() to DataDeleter
                self.delete_file(old_tr_file)
                self.delete_file(old_df_file)
                new_trs.append(tr)

        if not new_trs:
            self.logger.info('No new test data found after filtering out existing records')
            return

        # TODO: We should check if the device folder exists before creating it
        # Now we just do check processing in create_dev_dic()   
        devices_id_to_name = self.data_io.create_dev_dic(devs)

        # Get dataframes
        dfs = self.data_fetcher.get_dfs_from_trs(new_trs)
        # Save new test data and update directory structure
        self.data_io.save_test_data_update_dict(new_trs, dfs, devices_id_to_name)

