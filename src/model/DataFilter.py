from datetime import datetime

from src.logger_config import setup_logger
from src.model.DataIO import DataIO
from src.model.DirStructure import DirStructure
from src.constants import DATE_FORMAT, TIME_TOLERANCE

class DataFilter:
    """
    The class to filter data from the local disk

    Attributes
    ----------
    dataIO: DataIO object
        The object to save and load test data
    dirStructure: DirStructure object
        The object to manage the directory structure for the local data
    logger: logger object
        The object to log information
    
    Methods
    -------
    filter_trs(device_id=None, tr_name_substring=None, start_time=None, tags=None)
        Filter the test records with the specified device id or name or start time or tags
    filter_dfs(device_id=None, tr_name_substring=None, start_time=None, tags=None)
        Filter the dataframes with the specified device id or name or start time or tags
    filter_trs_and_dfs(device_id=None, tr_name_substring=None, start_time=None, tags=None)
        Filter the test records and dataframes with the specified device id or name or start time or tags
    filter_df_by_tr(tr, trace_keys=None)
        Filter the dataframe with the specified test record
    """
    def __init__(self, dataIO: DataIO, dirStructure: DirStructure):
        self.dataIO = dataIO
        self.dirStructure = dirStructure
        self.logger = setup_logger()
    
    def __filter_records(self, device_id=None, tr_name_substring=None, start_time=None, tags=None):
        """
        Fileter the records with the specified device id or name, and start time

        Parameters
        ----------
        device_id: str, optional
            The device id of the record to be found
        tr_name_substring: str, optional
            The substring of the device name of the record to be found
        start_time: str, optional
            The start time of the record to be found, in the format of 'YYYY-MM-DD_HH-MM-SS'
        tags: list of str, optional
            The list of tags of the record to be found
        
        Returns
        -------
        list of dict
            The list of records that match the specified device id or name, and start time
        """
        self.logger.info(f"Finding records with device_id={device_id}, tr_name_substring={tr_name_substring}, start_time={start_time}, tags={tags}") 
        if start_time is not None:
            start_time = datetime.strptime(start_time, DATE_FORMAT)

        dir_structure = self.dirStructure.load_records()

        matching_records = [
            record
            for record in dir_structure
            if (device_id is None or record['device_id'] == device_id) and
            (tr_name_substring is None or tr_name_substring in record['tr_name']) and
            (start_time is None or abs(datetime.strptime(record['start_time'], DATE_FORMAT) - start_time) <= TIME_TOLERANCE) and
            (tags is None or all(tag in record['tags'] for tag in tags))
        ]
        self.logger.info(f"Found {len(matching_records)} matching records")
        return matching_records

    def filter_trs(self, device_id=None, tr_name_substring=None, start_time=None, tags=None):
        """
        Filter the test records with the specified device id or name, and start time

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
        list of TestRecord object
            The list of test records that match the specified device id or name, and start time
        """
        matching_records = self.__filter_records(device_id=device_id, tr_name_substring=tr_name_substring, start_time=start_time, tags=tags)
        matching_test_folders = [record['test_folder'] for record in matching_records]
        return self.dataIO.load_trs(matching_test_folders)

    def filter_dfs(self, device_id=None, tr_name_substring=None, start_time=None, tags=None):
        """
        Filter the dataframes with the specified device id or name, and start time

        Parameters
        ----------
        device_id: str, optional
            The device id of the dataframe to be found
        tr_name_substring: str, optional
            The substring of the device name of the dataframe to be found
        start_time: str, optional
            The start time of the dataframe to be found, in the format of 'YYYY-MM-DD_HH-MM-SS'
        tags: list of str, optional
            The list of tags of the dataframe to be found
        
        Returns
        -------
        list of Dataframe
            The list of dataframes that match the specified device id or name, and start time
        """
        matching_records = self.__filter_records(device_id=device_id, tr_name_substring=tr_name_substring, start_time=start_time, tags=tags)
        matching_test_folders = [record['test_folder'] for record in matching_records]
        return self.dataIO.load_dfs(matching_test_folders)

    def filter_trs_and_dfs(self, device_id=None, tr_name_substring=None, start_time=None, tags=None):
        """
        Filter the test records and dataframes with the specified device id or name, start time and tags

        Parameters
        ----------
        device_id: str, optional
            The device id of the dataframe to be found
        tr_name_substring: str, optional
            The substring of the device name of the dataframe to be found
        start_time: str, optional
            The start time of the dataframe to be found, in the format of 'YYYY-MM-DD_HH-MM-SS'
        tags: list of str, optional
            The list of tags of the dataframe to be found
        
        Returns
        -------
        list of TestRecord object
            The list of test records that match the specified device id or name, start time and tags
        list of Dataframe
            The list of dataframes that match the specified device id or name, start time and tags
        """
        matching_records = self.__filter_records(device_id=device_id, tr_name_substring=tr_name_substring, start_time=start_time, tags=tags)
        matching_test_folders = [record['test_folder'] for record in matching_records]
        return self.dataIO.load_trs(matching_test_folders), self.dataIO.load_dfs(matching_test_folders)

    def filter_df_by_tr(self, tr, trace_keys=None):
        """
        Filter the dataframe with the specified test record

        Parameters
        ----------
        tr: TestRecord object
            The test record to be found
        trace_keys: list of str, optional
            The list of trace keys to be found
        
        Returns
        -------
        DataFrame
            The dataframe that matches the specified test record
        """
        self.logger.info(f"Finding dataframe that matches test record {tr.uuid}")
        dir_structure = self.dirStructure.load_records()
        matching_test_folder = ""
        for record in dir_structure:
            if record['uuid'] == tr.uuid:
                matching_test_folder = record['test_folder']
                break
        if matching_test_folder == "":
            self.logger.info(f"No dataframe found that matches test record {tr.uuid}, need to update the local data")
            return None
        self.logger.info(f"Found dataframe that matches test record {tr.uuid}")
        return self.dataIO.load_df(matching_test_folder, trace_keys=trace_keys)