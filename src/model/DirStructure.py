import os
import json
from src.constants import JSON_FILE_PATH, DATE_FORMAT
from logger_config import setup_logger

class DirStructure:
    """
    The class to manage the directory structure for the local data

    Attributes
    ----------
    filepath: str
        The path of the json file to store the directory structure
    structure: list of dict
        The list of records in the directory structure
    logger: logger object
        The object to log information

    Methods
    -------
    append_from_record(tr, dev_name, tr_path, df_path)
        Append a record to the directory structure
    load_records()
        Load the records from the directory structure
    load_uuid()
        Load the uuids from the directory structure
    load_dev_name()
        Load the device names from the directory structure
    load_uuid_to_last_dp_timestamp()
        Load the uuids and last data point timestamps from the directory structure
    load_uuid_to_tr_path_and_df_path()
        Load the uuids and test record and dataframe paths from the directory structure
    """
    def __init__(self):
        self.filepath = JSON_FILE_PATH
        self.logger = setup_logger()
        if not os.path.exists(self.filepath):
            self.structure = []
            self.__save()
        else:
            self.structure = self.__load()

    def __load(self):
        try:
            with open(self.filepath, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f'Error while loading directory structure: {e}')
            return []

    def __save(self):
        try:
            with open(self.filepath, 'w') as f:
                json.dump(self.structure, f, indent=4)
        except Exception as e:
            self.logger.error(f'Error while saving directory structure: {e}')

    def append_from_record(self, tr, dev_name, tr_path, df_path):
        try:
            self.structure.append({
                'uuid': tr.uuid,
                'device_id': tr.device_id,
                'tr_name': tr.name,  
                'dev_name': dev_name,
                'start_time': tr.start_time.strftime(DATE_FORMAT),
                'last_dp_timestamp': tr.last_dp_timestamp,
                'tr_path': tr_path,
                'df_path': df_path,
                'tags': tr.tags
            })
            self.__save()
        except Exception as e:
            self.logger.error(f'Error while appending directory structure: {e}')

    def load_records(self):
        return self.structure

    def load_uuid(self):
        return {record['uuid'] for record in self.structure}

    def load_dev_name(self):
        return {record['dev_name'] for record in self.structure}
    
    def load_uuid_to_last_dp_timestamp(self):
        return {record['uuid']: record['last_dp_timestamp'] for record in self.structure}

    def load_uuid_to_tr_path_and_df_path(self):
        return {record['uuid']: (record['tr_path'], record['df_path']) for record in self.structure}
    