import os
import pickle
from src.model.DirStructure import DirStructure
from src.constants import ROOT_PATH, DATE_FORMAT
from src.logger_config import setup_logger

class DataIO:
    """
    The class to save and load test data

    Attributes
    ----------
    rootPath: str
        The root path of the local data
    dirStructure: DirStructure object
        The object to manage the directory structure for the local data
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
    load_df(df_path, trace_keys=None)
        Load the dataframe from the pickle file
    load_trs(tr_paths)
        Load the test records from the pickle files
    load_dfs(df_paths)
        Load the dataframes from the pickle files
    """
    def __init__(self, dirStructure: DirStructure):
        self.rootPath = ROOT_PATH
        self.dirStructure = dirStructure
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
            The dictionary of device id and device name
        """
        devices_id_to_name = {}
        for dev in devs:
            device_folder = os.path.join(self.rootPath, dev.name)   
            self.__create_directory(device_folder)
            devices_id_to_name[dev.id] = dev.name
        return devices_id_to_name
    
    def save_test_data_update_dict(self, trs, dfs, devices_id_to_name):
        """
        Save test data to local disk and update the directory structure information
        
        Parameters
        ----------
        trs: list of TestRecord objects
            The list of test records to be saved
        dfs: list of pandas dataframe
            The list of dataframes to be saved
        devices_id_to_name: dict
            The dictionary of device id and device name

        Returns
        -------
        None
        """
        for tr, df in zip(trs, dfs):
            dev_name = devices_id_to_name[tr.device_id]
            self.__handle_single_record(tr, df, dev_name)
    
    def save_df(self, df, df_path):
        """
        Save the dataframe to a pickle file

        Parameters
        ----------
        df: pandas dataframe
            The dataframe to be saved
        df_path: str
            The path of the pickle file

        Returns
        -------
        None
        """
        self.__save_to_pickle(df, df_path)
    
    def __handle_single_record(self, tr, df, dev_name):
        device_folder = os.path.join(self.rootPath, dev_name)
        if device_folder is None:
            self.logger.error(f'Device folder not found for device id {tr.device_id}')
            return None

        start_time_str = tr.start_time.strftime(DATE_FORMAT)
        # last_modified_time_str = UNIX_timestamp_to_datetime(tr.last_dp_timestamp).strftime(DATE_FORMAT)

        test_folder = os.path.join(device_folder, start_time_str)
        if not os.path.exists(test_folder):
            self.__create_directory(test_folder)

        # Save the test data to a pickle file
        tr_path = self.dirStructure.get_tr_path(test_folder)
        self.__save_to_pickle(tr, tr_path)
        # Save the time series data to a pickle file
        df_path = self.dirStructure.get_df_path(test_folder)
        self.__save_to_pickle(df, df_path)

        # Append the directory structure information to the list
        self.dirStructure.append_from_record(tr, dev_name, test_folder)
    
    def __create_directory(self, directory_path):
        try:
            os.makedirs(directory_path, exist_ok=True)
        except Exception as err:
            self.logger.error(f'Error occurred while creating directory {directory_path}: {err}')

    def __save_to_pickle(self, data, file_path):
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'wb') as f:
                pickle.dump(data, f)
            self.logger.info(f'Saved pickle file to {file_path}')
        except Exception as err:
            self.logger.error(f'Error occurred while writing file {file_path}: {err}')
    
    def load_df(self, test_folder, trace_keys=None):
        """
        Load the dataframe from the pickle file with the specified trace keys

        Parameters
        ----------
        test_folder: str
            The path of the test folder
        trace_keys: list of str, optional
            The list of keys of the traces to be loaded

        Returns
        -------
        Dataframe
            The dataframe loaded from the pickle file
        """ 
        df_path = self.dirStructure.get_df_path(test_folder)
        df = self.__load_pickle(df_path)
        if trace_keys is not None:
            df = df[trace_keys]
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
        return self.__load_pickles(tr_paths)
    
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
        return self.__load_pickles(df_paths)

    def __load_pickles(self, file_paths):
        return [self.__load_pickle(file_path) for file_path in file_paths]

    def __load_pickle(self, file_path):
        try:
            with open(file_path, "rb") as f:
                record = pickle.load(f)
            self.logger.info(f"Loaded pickle file from {file_path}")
            return record
        except FileNotFoundError:
            self.logger.error(f"File not found: {file_path}")
            return None