from src.model.DirStructure import DirStructure
from src.model.DataFetcher import DataFetcher
from src.model.DataIO import DataIO
from src.model.DataDeleter import DataDeleter
from src.model.DataFilter import DataFilter
from src.model.DataProcessor import DataProcessor
from src.utils.Logger import setup_logger
from src.utils.SinglentonMeta import SingletonABCMeta
from src.utils.DateConverter import DateConverter
from src.utils.ObserverPattern import Subject, Observer
import os

class DataManager(Subject, metaclass=SingletonABCMeta):
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
    attach(observer: Observer)
        Attach an observer to the DataManager.
    detach(observer: Observer)
        Detach an observer from the DataManager.
    notify(*args, **kwargs)
        Notify all observers about an event.
    filter_trs(device_id=None, tr_name_substring=None, start_time=None, tags=None)
        Filter the test records locally with the specified device id or name or start time or tags
    filter_dfs(device_id=None, tr_name_substring=None, start_time=None, tags=None)
        Filter the dataframes locally with the specified device id or name or start time or tags
    filter_trs_and_dfs(device_id=None, tr_name_substring=None, start_time=None, tags=None)
        Filter the test records and dataframes locally with the specified device id or name or start time or tags
    process_cell(cell_name, numFiles = 1000)
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
        self._observers = []
        DataManager._is_initialized = True

    def attach(self, observer: Observer):
        self._observers.append(observer)

    def detach(self, observer: Observer):
        self._observers.remove(observer)

    def notify(self, *args, **kwargs):
        for observer in self._observers:
            observer.update(*args, **kwargs)
    

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
        update_local_db: bool
            Whether to update the local database before processing the cell
        
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
        cell_data_rpt = None
        if update:
            cell_data_rpt = self.dataProcessor.summarize_rpt_data(cell_data, cell_data_vdf, cell_cycle_metrics)
            self.dataIO.save_df(cell_cycle_metrics, filepath_ccm)
            self.dataIO.save_df(cell_data, filepath_cell_data)
            self.dataIO.save_df(cell_data_vdf, filepath_cell_data_vdf)  
            self.dataIO.save_df(cell_data_rpt, filepath_rpt)
        self.notify(cell_name, cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt)
        return cell_cycle_metrics, cell_data, cell_data_vdf, cell_data_rpt
   