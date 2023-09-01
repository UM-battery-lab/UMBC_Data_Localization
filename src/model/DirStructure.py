import os
import json
from src.config.time_config import DATE_FORMAT
from src.config.path_config import JSON_FILE_PATH, ROOT_PATH
from src.utils.Logger import setup_logger

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
    append_record(tr, dev_name, tr_path, df_path)
        Append a record to the directory structure and save it to the json file
    load_records()
        Load all the records information from the directory structure
    load_uuid()
        Load all the uuids from the directory structure
    load_test_folders()
        Load all the test folder paths from the directory structure
    load_dev_name()
        Load all the device names from the directory structure
    load_uuid_to_last_dp_timestamp()
        Load the dictionary of all the uuids to last data point timestamps from the directory structure
    load_uuid_to_tr_path_and_df_path()
        Load the dictionary of all the uuids to test record and dataframe paths from the directory structure
    load_dev_folder(dev_name)
        Load the device folder path from the directory structure by the device name
    get_tr_path(test_folder)
        Get the test record path from the directory structure by the test folder path
    get_df_path(test_folder)
        Get the dataframe path from the directory structure by the test folder path
    delete_record(uuid=None, test_folder=None)
        Delete the record from the directory structure by the uuid or test folder path
    """
    def __init__(self):
        self.filepath = JSON_FILE_PATH
        self.rootpath = ROOT_PATH
        self.logger = setup_logger()
        self.valid_keys = {'uuid', 'device_id', 'tr_name', 'dev_name', 'start_time', 'last_dp_timestamp', 'tags'}
        if not os.path.exists(self.filepath):
            self.structure = []
            self._save()
        else:
            self.structure = self._load()

    def _load(self):
        try:
            with open(self.filepath, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f'Error while loading directory structure: {e}')
            return []

    def _save(self):
        try:
            os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
            with open(self.filepath, 'w') as f:
                json.dump(self.structure, f, indent=4)
        except Exception as e:
            self.logger.error(f'Error while saving directory structure: {e}')

    def append_record(self, tr, dev_name):
        record = {
            'uuid': tr.uuid,
            'device_id': tr.device_id,
            'tr_name': tr.name,  
            'dev_name': dev_name,
            'start_time': tr.start_time.strftime(DATE_FORMAT),
            'last_dp_timestamp': tr.last_dp_timestamp,
            'tags': tr.tags
        }
        self.structure.append(record)   # First, append the new record to the structure
        try:
            self._save()  # Then, try to save the structure
        except Exception as e:
            self.logger.error(f'Error while saving directory structure: {e}')
            self._rollback()  # Rollback the changes if save fails

    def check_records(self):
        # Specify the set of keys that should be present in each record
        for record in self.structure:
            # Get the keys that are in the record but not in the valid_keys set
            invalid_keys = set(record.keys()) - self.valid_keys
            # Remove the invalid keys from the record
            for key in invalid_keys:
                self.logger.warning(f"Invalid key {key} in record {record['uuid']}")
                del record[key]
            # Add the missing keys to the record
            for key in self.valid_keys - set(record.keys()):
                self.logger.warning(f"Missing key {key} in record {record['uuid']}")
                record[key] = None
        self._save()

    def _rollback(self):
        """Remove the last added record."""
        if self.structure:
            self.structure.pop()

    def get_test_folder(self, record):
        return os.path.join(self.rootpath, record['dev_name'], record['start_time'])

    def load_records(self):
        return self.structure

    def load_uuid(self):
        return {record['uuid'] for record in self.structure}
    
    def load_test_folders(self):
        test_folders = []
        for record in self.structure:
            test_folders.append(self.get_test_folder(record))
        return test_folders

    def load_dev_name(self):
        return {record['dev_name'] for record in self.structure}
    
    def load_uuid_to_last_dp_timestamp(self):
        return {record['uuid']: record['last_dp_timestamp'] for record in self.structure}

    def load_uuid_to_tr_path_and_df_path(self):
        return {record['uuid']: (self.get_tr_path(self.get_test_folder(record)), 
                                 self.get_df_path(self.get_test_folder(record))) for record in self.structure}
    
    def load_dev_folder(self, dev_name):
        for record in self.structure:
            if record['dev_name'] == dev_name:
                return os.path.join(self.rootpath, record['dev_name'])
        self.logger.warning(f"No related test record for {dev_name}")
        return None
    
    def get_tr_path(self, test_folder):
        return os.path.join(test_folder, 'tr.pkl.gz')
    
    def get_df_path(self, test_folder):
        return os.path.join(test_folder, 'df.pkl.gz')
    
    def delete_record(self, uuid=None, test_folder=None):
        # Filter out records based on provided uuid or test_folder
        if uuid:
            self.structure = [record for record in self.structure if record['uuid'] != uuid]
        elif test_folder:
            self.structure = [record for record in self.structure if self.get_test_folder(record) != test_folder]
        self._save()