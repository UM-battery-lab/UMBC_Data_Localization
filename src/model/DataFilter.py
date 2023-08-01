from datetime import datetime

from logger_config import setup_logger
from src.model.DataIO import DataIO
from src.constants import DATE_FORMAT, TIME_TOLERANCE

class DataFilter:
    """
    The class to filter data from the local disk

    Attributes
    ----------
    dataIO: DataIO object
        The object to save and load data
    logger: logger object
        The object to log information
    
    Methods
    -------
    filter_trs(device_id=None, device_name_substring=None, start_time=None)
        Filter the test records with the specified device id or name, and start time
    filter_dfs(device_id=None, device_name_substring=None, start_time=None)
        Filter the dataframes with the specified device id or name, and start time
    filter_trs_and_dfs(device_id=None, device_name_substring=None, start_time=None)
        Filter the test records and dataframes with the specified device id or name, and start time
    """
    def __init__(self, dataIO: DataIO):
        self.dataIO = dataIO
        self.logger = setup_logger()
    
    def __filter_records(self, device_id=None, device_name_substring=None, start_time=None):
        """
        Fileter the records with the specified device id or name, and start time

        Parameters
        ----------
        device_id: str, optional
            The device id of the record to be found
        device_name_substring: str, optional
            The substring of the device name of the record to be found
        start_time: str, optional
            The start time of the record to be found, in the format of 'YYYY-MM-DD_HH-MM-SS'
        
        Returns
        -------
        list of dict
            The list of records that match the specified device id or name, and start time
        """
        self.logger.info(f"Finding records with device_id={device_id}, device_name_substring={device_name_substring}, start_time={start_time}")
        
        dir_structure = self.dataIO.load_directory_structure()
        
        if start_time is not None:
            start_time = datetime.strptime(start_time, DATE_FORMAT)
        
        matching_records = [
            record
            for record in dir_structure
            if (device_id is None or record['device_id'] == device_id) and
            (device_name_substring is None or device_name_substring in record['tr_name']) and
            (start_time is None or abs(datetime.strptime(record['start_time'], DATE_FORMAT) - start_time) <= TIME_TOLERANCE)
        ]
        self.logger.info(f"Found {len(matching_records)} matching records")
        return matching_records

    def filter_trs(self, device_id=None, device_name_substring=None, start_time=None):
        """
        Filter the test records with the specified device id or name, and start time

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
        matching_records = self.__filter_records(device_id=device_id, device_name_substring=device_name_substring, start_time=start_time)
        return [record['tr_path'] for record in matching_records]

    def filter_dfs(self, device_id=None, device_name_substring=None, start_time=None):
        """
        Filter the dataframes with the specified device id or name, and start time

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
        matching_records = self.__filter_records(device_id=device_id, device_name_substring=device_name_substring, start_time=start_time)
        return [record['df_path'] for record in matching_records]

    def filter_trs_and_dfs(self, device_id=None, device_name_substring=None, start_time=None):
        """
        Filter the test records and dataframes with the specified device id or name, and start time

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
        matching_records = self.__filter_records(device_id=device_id, device_name_substring=device_name_substring, start_time=start_time)
        return [record['tr_path'] for record in matching_records], [record['df_path'] for record in matching_records]

