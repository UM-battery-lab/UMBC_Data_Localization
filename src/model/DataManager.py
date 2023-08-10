from src.model.DirStructure import DirStructure
from src.model.DataFetcher import DataFetcher
from src.model.DataIO import DataIO
from src.model.DataDeleter import DataDeleter
from src.model.DataFilter import DataFilter
from src.model.DataProcessor import DataProcessor
from src.logger_config import setup_logger
import os


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
    filter_trs(device_id=None, tr_name_substring=None, start_time=None, tags=None)
        Filter the test records locally with the specified device id or name or start time or tags
    filter_dfs(device_id=None, tr_name_substring=None, start_time=None, tags=None)
        Filter the dataframes locally with the specified device id or name or start time or tags
    filter_trs_and_dfs(device_id=None, tr_name_substring=None, start_time=None, tags=None)
        Filter the test records and dataframes locally with the specified device id or name or start time or tags
    process_cell(cell_name, numFiles = 1000)
        Process the data for a cell and save the processed data to local disk
    """
    def __init__(self):
        self.dirStructure = DirStructure()
        self.dataIO = DataIO(self.dirStructure)
        self.dataFetcher = DataFetcher()
        self.dataDeleter = DataDeleter()
        self.dataFilter = DataFilter(self.dataIO, self.dirStructure)
        self.dataProcessor = DataProcessor(self.dataFilter, self.dirStructure)
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
        # Save test data and update directory structure
        self.__update_batch_data(trs, device_id_to_name)

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
                self.dirStructure.delete_record(tr.uuid)
                new_trs.append(tr)

        if not new_trs:
            self.logger.info('No new test data found after filtering out existing records')
            return

        devices_id_to_name = self.dataIO.create_dev_dic(devs)
        self.__update_batch_data(new_trs, devices_id_to_name)
    
    def __update_batch_data(self, new_trs, devices_id_to_name, batch_size=20):
        for i in range(0, len(new_trs), batch_size):
            new_trs_batch = new_trs[i:i+batch_size]
            # Get dataframes 
            dfs_batch = self.dataFetcher.get_dfs_from_trs(new_trs_batch)
            # Save new test data and update directory structure
            self.dataIO.save_test_data_update_dict(new_trs_batch, dfs_batch, devices_id_to_name)

    def filter_trs(self, device_id=None, tr_name_substring=None, start_time=None, tags=None):
        """
        Filter the test records locally with the specified device id or name, and start time

        Parameters
        ----------
        device_id: str, optional
            The device id of the test record to be found
        tr_name_substring: str, optional
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
        return self.dataFilter.filter_trs(device_id, tr_name_substring, start_time, tags)
    
    def filter_dfs(self, device_id=None, tr_name_substring=None, start_time=None, tags=None):
        """
        Filter the dataframes locally with the specified device id or name, and start time

        Parameters
        ----------
        device_id: str, optional
            The device id of the dataframe to be found
        tr_name_substring: str, optional
            The substring of the device name of the dataframe to be found
        start_time: str, optional
            The start time of the dataframe to be found, in the format of 'YYYY-MM-DD_HH-MM-SS'
        
        Returns
        -------
        list of dataframes
            The list of dataframes that match the specified device id or name, and start time and tags
        """
        return self.dataFilter.filter_dfs(device_id, tr_name_substring, start_time, tags)
    

    def filter_trs_and_dfs(self, device_id=None, tr_name_substring=None, start_time=None, tags=None):
        """
        Filter the test records and dataframes locally with the specified device id or name, and start time

        Parameters
        ----------
        device_id: str, optional
            The device id of the dataframe to be found
        tr_name_substring: str, optional
            The substring of the tr name of the dataframe to be found
        start_time: str, optional
            The start time of the dataframe to be found, in the format of 'YYYY-MM-DD_HH-MM-SS'
        
        Returns
        -------
        list of test records
            The list of test records that match the specified device id or name, and start time and tags
        list of dataframes
            The list of dataframes that match the specified device id or name, and start time and tags
        """
        return self.dataFilter.filter_trs_and_dfs(device_id, tr_name_substring, start_time, tags)

    def process_cell(self, cell_name, numFiles = 1000):
        """
        Process the data for a cell and save the processed data to local disk

        Parameters
        ----------
        cell_name: str
            The name of the cell to be processed
        numFiles: int
            The number of files to be processed
        
        Returns
        -------
        cell_cycle_metrics: dataframe
            The dataframe of cycle metrics for the cell
        cell_data: dataframe
            The dataframe of cell data for the cell
        cell_data_vdf: dataframe
            The dataframe of cell data vdf for the cell
        """
        cell_path = self.dirStructure.load_dev_folder(cell_name)
        # Filepaths for cycle metrics, cell data, cell data vdf and rpt
        filepath_ccm = os.path.join(cell_path, 'CCM.pickle')
        filepath_cell_data = os.path.join(cell_path, 'CD.pickle')
        filepath_cell_data_vdf = os.path.join(cell_path, 'CDvdf.pickle')
        filepath_rpt = os.path.join(cell_path, 'RPT.pickle')
        # Load dataframes for cycle metrics, cell data, cell data vdf
        cell_cycle_metrics = self.dataIO.load_df(df_path=filepath_ccm)
        cell_data = self.dataIO.load_df(df_path=filepath_cell_data)
        cell_data_vdf = self.dataIO.load_df(df_path=filepath_cell_data_vdf)
        # Load trs for cycler data
        trs_neware = self.dataFilter.filter_trs(tr_name_substring=cell_name, tags=['neware_xls_4000'])
        trs_arbin = self.dataFilter.filter_trs(tr_name_substring=cell_name, tags=['arbin'])
        trs_biologic = self.dataFilter.filter_trs(tr_name_substring=cell_name, tags=['biologic'])
        trs_vdf = self.dataFilter.filter_trs(tr_name_substring=cell_name, tags=['vdf'])        
        # Sort trs
        trs_neware = self.dataProcessor.sort_tests(trs_neware)
        trs_arbin = self.dataProcessor.sort_tests(trs_arbin)
        trs_biologic = self.dataProcessor.sort_tests(trs_biologic)
        trs_cycler = self.dataProcessor.sort_tests(trs_neware + trs_arbin + trs_biologic)
        trs_vdf = self.dataProcessor.sort_tests(trs_vdf)
        # Process data
        cell_cycle_metrics, cell_data, cell_data_vdf, update = self.dataProcessor.process_cell(trs_cycler, trs_vdf, cell_cycle_metrics, cell_data, cell_data_vdf, numFiles)
        #Save new data to pickle if there was new data
        if update:
            cell_rpt_data = self.dataProcessor.summarize_rpt_data(cell_data, cell_data_vdf, cell_cycle_metrics)
            self.dataIO.save_df(cell_cycle_metrics, filepath_ccm)
            self.dataIO.save_df(cell_data, filepath_cell_data)
            self.dataIO.save_df(cell_data_vdf, filepath_cell_data_vdf)  
            self.dataIO.save_df(cell_rpt_data, filepath_rpt)
        return cell_cycle_metrics, cell_data, cell_data_vdf
    
    def get_concatenated_data(self, device_id, trs_names=None):
        return self.dataProcessor.get_concatenated_data(device_id, trs_names)  

    # Below are the methods for testing
    def test_createdb(self):
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

        test_trs = trs[:50]
        
        if trs is None or devs is None:
            self.logger.error('Failed to fetch data')
            return
        
        # Create device folder dictionary
        device_id_to_name = self.dataIO.create_dev_dic(devs)
        # Save test data and update directory structure
        self.__update_batch_data(test_trs, device_id_to_name)

    def test_updatedb(self):
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
        test_trs = trs[:250]
        devs = self.dataFetcher.fetch_devs()
        self.update_test_data(test_trs, devs)