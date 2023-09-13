from src.model.DirStructure import DirStructure
from src.model.DataFetcher import DataFetcher
from src.model.DataIO import DataIO
from src.model.DataDeleter import DataDeleter
from src.model.DataFilter import DataFilter
from src.model.DataProcessor import DataProcessor
from src.utils.Logger import setup_logger
from src.utils.SinglentonMeta import SingletonMeta
from src.utils.DateConverter import DateConverter
from src.utils.ObserverPattern import Subject
import os
import gc

@Subject
class DataManager(metaclass=SingletonMeta):
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
    _createdb()
        Create the local database with all the test records and devices
    _updatedb(device_id=None, project_name=None, start_before=None, start_after=None)
        Update the local database with all the test records
    update_test_data(trs=None, devs=None, batch_size=60)
        Update the test data and directory structure with the specified test records and devices
    filter_trs(device_id=None, tr_name_substring=None, start_time=None, tags=None)
        Filter the test records locally with the specified device id or name or start time or tags
    filter_dfs(device_id=None, tr_name_substring=None, start_time=None, tags=None)
        Filter the dataframes locally with the specified device id or name or start time or tags
    filter_trs_and_dfs(device_id=None, tr_name_substring=None, start_time=None, tags=None)
        Filter the test records and dataframes locally with the specified device id or name or start time or tags
    check_and_repair_consistency()
        Check the consistency between the directory structure and local database, and repair the inconsistency
    process_cell(cell_name, numFiles = 1000, update_local_db=False)
        Process the data for a cell and save the processed cell cycle metrics, cell data and cell data vdf to local disk
    """
    _is_initialized = False
    def __init__(self, use_redis=False):
        if DataManager._is_initialized:
            return
        self.dirStructure = DirStructure()
        self.dataFetcher = DataFetcher()
        self.dataDeleter = DataDeleter()
        self.dataIO = DataIO(self.dirStructure, self.dataDeleter, use_redis)
        self.dataFilter = DataFilter(self.dataIO, self.dirStructure)
        self.dataProcessor = DataProcessor(self.dataFilter, self.dirStructure)
        self.dataConverter = DateConverter()
        self.logger = setup_logger()
        DataManager._is_initialized = True

    def _createdb(self):
        """
        Create the local database with all the test records and devices

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
        devices_id, devices_name, projects_name = self.dataIO.create_dev_dic(devs)
        # Save test data and update directory structure
        self._update_batch_data(trs, devices_id, devices_name, projects_name)

    def _updatedb(self, device_id=None, project_name=None, start_before=None, start_after=None):
        """
        Update the local database with all the test records

        Parameters
        ----------
        device_id: int, optional
            The device id to be updated
        project_name: str, optional
            The project name to be updated
        start_before: str, optional
            The start time of test records to be updated should be earlier than this time, in the format of 'YYYY-MM-DD_HH-MM-SS'
        start_after: str, optional
            The start time of test records to be updated should be later than this time, in the format of 'YYYY-MM-DD_HH-MM-SS'        

        Returns
        -------
        None
        """
        # Fetch test records and devices
        trs = self.dataFetcher.fetch_trs()
        if device_id:
            trs = [tr for tr in trs if tr.device_id == device_id]
        if project_name:
            project_devices_id = self.dirStructure.get_project_devices_id(project_name)
            trs = [tr for tr in trs if tr.device_id in project_devices_id]
        if start_before:
            start_before = self.dataConverter._str_to_timestamp(start_before)
            trs = [tr for tr in trs if self.dataConverter._datetime_to_timestamp(tr.start_time) < start_before]
        if start_after:
            start_after = self.dataConverter._str_to_timestamp(start_after)
            trs = [tr for tr in trs if self.dataConverter._datetime_to_timestamp(tr.start_time) > start_after]
        self.logger.info(f'Find {len(trs)} test records meeting the criteria')
        devs = self.dataFetcher.fetch_devs()
        self.update_test_data(trs, devs, len(trs))
    
    def update_test_data(self, trs=None, devs=None, num_new_trs=60):
        """
        Update the test data and directory structure with the specified test records and devices

        Parameters
        ----------
        trs: list of TestRecord objects (optional)
            The list of test records to be saved
        devs: list of Device objects (optional)
            The list of devices to be saved
        num_new_trs: int
            The number of test records to be updated
        
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
                if len(new_trs) >= num_new_trs:
                    break

        if not new_trs:
            self.logger.info('No new test data found after filtering out existing records')
            return

        devices_id, devices_name, projects_name  = self.dataIO.create_dev_dic(devs)
        self.dirStructure.update_project_devices(devices_id, devices_name, projects_name)
        self._update_batch_data(new_trs, devices_id, devices_name, projects_name) 
    
    def _update_batch_data(self, new_trs, devices_id, devices_name, projects_name, batch_size=5):
        for i in range(0, len(new_trs), batch_size):
            new_trs_batch = new_trs[i:i+batch_size]
            # Get dataframes 
            dfs_batch = self.dataFetcher.get_dfs_from_trs(new_trs_batch)
            # Save new test data and update directory structure
            self.dataIO.save_test_data_update_dict(new_trs_batch, dfs_batch, devices_id, devices_name, projects_name)
            gc.collect()

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
    
    def check_and_repair_consistency(self):
        """
        Check the consistency between the directory structure and local database, and repair the inconsistency

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self.logger.info('Starting consistency check between directory structure and local database...')

        # Step 1: Check if the device folders are in the corrosponding peoject folders
        self.logger.info('Checking if device folders are in the corrosponding peoject folders...')
        devs = self.dataFetcher.fetch_devs()
        devices_id, devices_name, projects_name = self.dataIO.create_dev_dic(devs)
        for dev in devs:
            if '_' not in dev.name:
                continue
            if os.path.exists(os.path.join(self.dirStructure.rootPath, dev.name)):
                self.logger.warning(f'Found device folder {dev.name} not in the corrosponding peoject folder')
                src_folder = os.path.join(self.dirStructure.rootPath, dev.name)
                project_name = projects_name[devices_id.index(dev.id)]
                if project_name is None:
                    self.logger.error(f'No project name found for device {dev.name}')
                    continue
                self.logger.info(f'Moving device folder {dev.name} to project folder {project_name}')
                dst_folder = os.path.join(self.dirStructure.rootPath, project_name, dev.name)
                self.dataIO.merge_folders(src_folder, dst_folder)
        
        # Step 2: Check if the project name be recorded in the directory structure is the same as the project name in the tags
        self.dirStructure.check_project_name(devices_id, projects_name)
            
        # Step 3: Check for empty or incomplete folders and delete them.
        empty_folders, valid_folders = self.dataIO._check_folders()
        if empty_folders:
            self.logger.info(f'Empty or incomplete folders found: {empty_folders}')
            self.dataDeleter.delete_folders(empty_folders)

        # Convert to sets for easier operations
        valid_folders_set = set(valid_folders)
        recorded_folders_set = set(self.dirStructure.load_test_folders())

        # Step 4: Check for folders present on disk but not recorded in the directory structure.
        unrecorded_folders = valid_folders_set - recorded_folders_set
        if unrecorded_folders:
            self.logger.info(f'{len(unrecorded_folders)} folders not recorded in directory structure')
            trs = self.dataIO.load_trs(list(unrecorded_folders))
            for tr, test_folder in zip(trs, unrecorded_folders):
                if tr is None:
                    self.logger.info(f'No test record found for folder {test_folder}')
                    continue
                dev_name = devices_name[devices_id.index(tr.device_id)]
                project_name = projects_name[devices_id.index(tr.device_id)]
                if dev_name:
                    self.dirStructure.append_record(tr, dev_name, project_name)
                    self.logger.info(f'Appended record for folder {test_folder}')

        # Step 5: Check for records in the directory structure that don't have corresponding folders on disk.
        orphaned_records = recorded_folders_set - valid_folders_set
        if orphaned_records:
            self.logger.info(f'Orphaned records found without corresponding folders on disk: {orphaned_records}')
            for orphaned_record in orphaned_records:
                # Delete the orphaned record from the directory structure based on its test_folder.
                self.dirStructure.delete_record(test_folder=orphaned_record)
                self.logger.info(f'Deleted {len(orphaned_records)} orphaned records from directory structure.')

        self.logger.info('Consistency check completed.')

    def process_cell(self, cell_name, start_time=None, end_time=None, numFiles = 1000):
        """
        Process the data for a cell and save the processed data to local disk

        Parameters
        ----------
        cell_name: str
            The name of the cell to be processed
        numFiles: int
            The number of files to be processed
        start_time: str, optional
            The start time of the test records to be processed, in the format of 'YYYY-MM-DD_HH-MM-SS'
        end_time: str, optional
            The end time of the test records to be processed, in the format of 'YYYY-MM-DD_HH-MM-SS'
        
        Returns
        -------
        cell_cycle_metrics: dataframe
            The dataframe of cycle metrics for the cell
        cell_data: dataframe
            The dataframe of cell data for the cell
        cell_data_vdf: dataframe
            The dataframe of cell data vdf for the cell
        """
        cell_cycle_metrics, cell_data, cell_data_vdf, _ = self.load_processed_data(cell_name)
        # Load trs for cycler data
        #TODO： use device name, not tr_name_substring
        trs_neware = self.dataFilter.filter_trs(tr_name_substring=cell_name, tags=['neware_xls_4000'])
        trs_arbin = self.dataFilter.filter_trs(tr_name_substring=cell_name, tags=['arbin'])
        trs_biologic = self.dataFilter.filter_trs(tr_name_substring=cell_name, tags=['biologic'])
        trs_vdf = self.dataFilter.filter_trs(tr_name_substring=cell_name, tags=['vdf'])        
        # Sort trs
        if start_time:
            start_time = self.dataConverter._str_to_datetime(start_time)
        if end_time:
            end_time = self.dataConverter._str_to_datetime(end_time)   
        trs_neware = self.dataProcessor.sort_tests(trs_neware)
        trs_arbin = self.dataProcessor.sort_tests(trs_arbin)
        trs_biologic = self.dataProcessor.sort_tests(trs_biologic)
        trs_cycler = self.dataProcessor.sort_tests(trs_neware + trs_arbin + trs_biologic)
        trs_vdf = self.dataProcessor.sort_tests(trs_vdf)
        # Process data
        cell_cycle_metrics, cell_data, cell_data_vdf, update = self.dataProcessor.process_cell(trs_cycler, trs_vdf, cell_cycle_metrics, cell_data, cell_data_vdf, numFiles)
        #Save new data to pickle if there was new data
        cell_data_rpt = None
        if update:
            cell_data_rpt = self.dataProcessor.summarize_rpt_data(cell_data, cell_data_vdf, cell_cycle_metrics)
            self.dataIO.save_processed_data(cell_name, cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt)
        self.notify(cell_name, cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt, start_time, end_time)
        return cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt
   
    def load_processed_data(self, cell_name):
        """
        Get the processed data for a cell

        Parameters
        ----------
        cell_name: str
            The name of the cell to be processed

        Returns
        -------
        cell_cycle_metrics: dataframe
            The dataframe of cycle metrics for the cell
        cell_data: dataframe
            The dataframe of cell data for the cell
        cell_data_vdf: dataframe
            The dataframe of cell data vdf for the cell
        cell_data_rpt: dataframe
            The dataframe of cell data rpt for the cell
        """
        return self.dataIO.load_processed_data(cell_name)
        
    # Below are the methods for testing
    def test_createdb(self):
        """
        Create the local database for testing

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
        devices_id, devices_name, projects_name = self.dataIO.create_dev_dic(devs)
        # Save test data and update directory structure
        self._update_batch_data(test_trs, devices_id, devices_name, projects_name)

    def test_updatedb(self):
        """
        Update the local database for testing

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        # Fetch test records and devices
        trs = self.dataFetcher.fetch_trs()
        test_trs = trs[:1000]
        devs = self.dataFetcher.fetch_devs()
        self.update_test_data(test_trs, devs, len(test_trs))