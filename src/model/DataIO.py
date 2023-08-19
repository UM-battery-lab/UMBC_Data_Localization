import os
import pickle
import pandas as pd
import gzip
import shutil
from src.model.DirStructure import DirStructure
from src.model.DataDeleter import DataDeleter
from src.utils.constants import ROOT_PATH, DATE_FORMAT, TIME_COLUMNS
from src.utils.logger_config import setup_logger

class DataIO:
    """
    The class to save and load test data

    Attributes
    ----------
    rootPath: str
        The root path of the local data
    dirStructure: DirStructure object
        The object to manage the directory structure for the local data
    dataDeleter: DataDeleter object
        The object to delete data from the local disk
    logger: logger object
        The object to log information
        
    Methods
    -------
    create_dev_dic(devs)
        Create the path for each device and return a dictionary of device id and device folder path
    save_test_data_update_dict(trs, dfs, devices_id_to_name)
        Save test data to local disk and update the directory structure information
    save_df(df, df_path)
        Save the dataframe to a pickle file
    load_df(test_folder=None, df_path=None, trace_keys=None)
        Load the dataframe from the pickle file with the specified trace keys
    load_trs(test_folders)
        Load the test records based on the specified test folders
    load_dfs(test_folders)
        Load the dataframes based on the specified test folders
    """
    def __init__(self, dirStructure: DirStructure, dataDeleter: DataDeleter):
        self.rootPath = ROOT_PATH
        self.dirStructure = dirStructure
        self.dataDeleter = dataDeleter
        self.logger = setup_logger()

    def create_dev_dic(self, devs):
        """
        Create a dictionary of device id and device folder path

        Parameters
        ----------
        devs: list of Device objects
            The list of devices to be saved

        Returns
        -------
        dict
            The dictionary of device id to device name
        """
        devices_id_to_name = {}
        for dev in devs:
            device_folder = os.path.join(self.rootPath, dev.name)   
            self._create_directory(device_folder)
            devices_id_to_name[dev.id] = dev.name
        return devices_id_to_name
    
    def save_test_data_update_dict(self, trs, dfs, devices_id_to_name):
        """
        Save test data to local disk and update the directory structure information
        
        Parameters
        ----------
        trs: list of TestRecord objects
            The list of test records to be saved
        dfs: list of pandas Dataframe
            The list of dataframes to be saved
        devices_id_to_name: dict
            The dictionary of device id and device name

        Returns
        -------
        None
        """
        for tr, df in zip(trs, dfs):
            dev_name = devices_id_to_name[tr.device_id]
            self._handle_single_record(tr, df, dev_name)
    
    def save_df(self, df, df_path):
        """
        Save the dataframe to a pickle file

        Parameters
        ----------
        df: pandas Dataframe
            The dataframe to be saved
        df_path: str
            The path of the pickle file

        Returns
        -------
        None
        """
        self._save_to_pickle(df, df_path)
    
    def _handle_single_record(self, tr, df, dev_name):
        device_folder = os.path.join(self.rootPath, dev_name)
        if device_folder is None:
            self.logger.error(f'Device folder not found for device id {tr.device_id}')
            return None

        start_time_str = tr.start_time.strftime(DATE_FORMAT)
        # last_modified_time_str = UNIX_timestamp_to_datetime(tr.last_dp_timestamp).strftime(DATE_FORMAT)

        test_folder = os.path.join(device_folder, start_time_str)
        if not os.path.exists(test_folder):
            self._create_directory(test_folder)

        # Save the test data to a pickle file
        tr_path = self.dirStructure.get_tr_path(test_folder)
        # Save the time series data to a pickle file
        df_path = self.dirStructure.get_df_path(test_folder)
        if tr is None or df is None:
            self.logger.error(f'Test record or dataframe is None')
            return None
        try:
            # Guarantee the transactional integrity
            self._save_to_pickle(tr, tr_path)
            self._save_to_pickle(df, df_path)
            # Append the directory structure information to the list
            self.dirStructure.append_record(tr, dev_name, test_folder)
        except Exception as e:
            self.logger.error(f"Transaction failed: {e}")
            # Remove any possibly corrupted files
            for path in [tr_path, df_path]:
                if os.path.exists(path):
                    self.dataDeleter.delete_file(path)
            return None
    
    def _create_directory(self, directory_path):
        try:
            os.makedirs(directory_path, exist_ok=True)
        except Exception as err:
            self.logger.error(f'Error occurred while creating directory {directory_path}: {err}')

    def _save_to_pickle(self, data, file_path):
        temp_path = file_path + ".tmp"
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with gzip.open(temp_path, 'wb') as f:
                pickle.dump(data, f)
            shutil.move(temp_path, file_path)
            self.logger.info(f'Saved pickle file to {file_path}')
        except Exception as err:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            self.logger.error(f'Error occurred while writing file {file_path}: {err}')

    def _check_time_column_in_trace_keys(self, df, trace_keys):
        # Find invalid time columns in trace_keys
        error_time_keys = set(trace_keys) & (set(TIME_COLUMNS) - set(df.columns))   
        if error_time_keys:
            self.logger.warning(f'Trace keys contain invalid time columns: {list(error_time_keys)}')    
            # Find an available time column from df.columns to replace the error time column
            replace_time_keys = set(TIME_COLUMNS) & set(df.columns)
            if replace_time_keys:
                replace_time_key = list(replace_time_keys)[0]
                self.logger.warning(f'Replacing with time column: {replace_time_key}')
                trace_keys = [replace_time_key if key in error_time_keys else key for key in trace_keys]
            else:
                self.logger.error("No valid time column found in dataframe.")          
        return trace_keys

    
    def load_df(self, test_folder=None, df_path=None, trace_keys=None):
        """
        Load the dataframe from the pickle file with the specified trace keys

        Parameters
        ----------
        test_folder: str
            The path of the test folder
        df_path: str, optional
            The path of the pickle file
        trace_keys: list of str, optional
            The list of keys of the traces to be loaded

        Returns
        -------
        Dataframe
            The dataframe loaded from the pickle file
        """ 
        if df_path is None:
            if test_folder is None:
                self.logger.error('Either test_folder or df_path must be specified')
                return None
            df_path = self.dirStructure.get_df_path(test_folder)
        df = self._load_pickle(df_path)
        if df is None:
            self.logger.error(f"DataFrame is None when attempting to load from {df_path}")
            return None
        if trace_keys is not None:
            try:
                trace_keys = self._check_time_column_in_trace_keys(df, trace_keys)
                df = df[trace_keys]
            except KeyError as err:
                self.logger.error(f'Error occurred while loading dataframe: {err} with trace keys {trace_keys}')
                return None
            except TypeError:
                self.logger.error(f"DataFrame is None when attempting to filter by trace keys: {trace_keys}")
                return None
        return df

    def load_trs(self, test_folders):
        """
        Load the test records based on the specified test folders

        Parameters
        ----------
        test_folders: list of str
            The list of paths of the test folders
        
        Returns
        -------
        list of TestRecord objects
            The list of test records loaded from the pickle files
        """
        tr_paths = [self.dirStructure.get_tr_path(test_folder) for test_folder in test_folders]
        return self._load_pickles(tr_paths)
    
    def load_dfs(self, test_folders):
        """
        Load the dataframes based on the specified test folders

        Parameters
        ----------
        test_folders: list of str
            The list of paths of the test folders
        
        Returns
        ------- 
        list of Dataframe    
            The list of dataframes loaded from the pickle files
        """
        df_paths = [self.dirStructure.get_df_path(test_folder) for test_folder in test_folders]
        return self._load_pickles(df_paths)

    def _load_pickles(self, file_paths):
        return [self._load_pickle(file_path) for file_path in file_paths]

    def _load_pickle(self, file_path):
        try:
            with gzip.open(file_path, "rb") as f:
                record = pickle.load(f)
            self.logger.info(f"Loaded pickle file from {file_path}")
            return record
        except FileNotFoundError:
            self.logger.error(f"File not found: {file_path}")
            return None