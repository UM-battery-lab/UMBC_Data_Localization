import os
import json
from src.config.time_config import DATE_FORMAT
from src.config.path_config import DIR_STRUCTURE_PATH, ROOT_PATH, PROJECT_DEVICES_PATH
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
    check_records()
        Check the records in the directory structure and remove the invalid keys
    check_project_name(devices_id, projects_name)
        Check the project name for each device in the directory structure
    get_test_folder(record)
        Get the test folder path from the record
    get_project_folder(record)
        Get the project folder path from the record
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
    update_project_devices(devices_id, devices_name, projects_name)
        Update the project devices information in the project_devices.json file
    load_project_devices()
        Load the project devices information from the project_devices.json file
    get_project_devices_id(project_name)
        Get the list of device ids for the project name
    """
    def __init__(self):
        self.dirStructurePath = DIR_STRUCTURE_PATH
        self.rootPath = ROOT_PATH
        self.projectDevicesPath = PROJECT_DEVICES_PATH
        self.logger = setup_logger()
        self.validKeys = {'uuid', 'device_id', 'tr_name', 'dev_name', 'start_time', 'last_dp_timestamp', 'tags'}
        if not os.path.exists(self.dirStructurePath):
            self.structure = []
            self._save(self.dirStructurePath, self.structure)
        else:
            self.structure = self._load(self.dirStructurePath)

    def _load(self, path):
        try:
            with open(path, 'r') as f:
                self.logger.info(f'Loading json file from {path}')
                return json.load(f)
        except Exception as e:
            self.logger.error(f'Error while loading json file: {e}')
            return []

    def _save(self, path, data):
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, 'w') as f:
                self.logger.info(f'Saving json file to {path}')
                json.dump(data, f, indent=4)
        except Exception as e:
            self.logger.error(f'Error while saving json file: {e}')

    def append_record(self, tr, dev_name, project_name):
        try:
            record = {
                'uuid': tr.uuid,
                'device_id': tr.device_id,
                'tr_name': tr.name,  
                'dev_name': dev_name,
                'project_name': project_name,
                'start_time': tr.start_time.strftime(DATE_FORMAT),
                'last_dp_timestamp': tr.last_dp_timestamp,
                'tags': tr.tags
            }
            self.structure.append(record)   # First, append the new record to the structure
        except Exception as e:
            self.logger.error(f'Error while appending record: {e}')
        self.save_dir_structure()

    def save_dir_structure(self):
        try:
            self._save(self.dirStructurePath, self.structure)  # Then, try to save the structure
        except Exception as e:
            self.logger.error(f'Error while saving directory structure: {e}')
            self._rollback()  # Rollback the changes if save fails

    def check_records(self):
        # Specify the set of keys that should be present in each record
        for record in self.structure:
            # Get the keys that are in the record but not in the valid_keys set
            invalid_keys = set(record.keys()) - self.validKeys
            # Remove the invalid keys from the record
            for key in invalid_keys:
                self.logger.warning(f"Invalid key {key} in record {record['uuid']}")
                del record[key]
            # Add the missing keys to the record
            for key in self.validKeys - set(record.keys()):
                self.logger.warning(f"Missing key {key} in record {record['uuid']}")
                record[key] = None
        self._save(self.dirStructurePath, self.structure)
    
    def check_project_name(self, devices_id, projects_name, devices_name=None):
        self.logger.info(f"Checking project name for {len(devices_id)} devices")
        for record in self.structure:
            if record['device_id'] in devices_id:
                record['project_name'] = projects_name[devices_id.index(record['device_id'])]
                if devices_name:
                    record['dev_name'] = devices_name[devices_id.index(record['device_id'])]
            # TODO: Delete the actual data folder if the project name is not in the list
            else:
                self.logger.warning(f"Device {record['device_id']} is not in the list")
                # self.structure.remove(record)
        self._save(self.dirStructurePath, self.structure)

    def _rollback(self):
        """Remove the last added record."""
        if self.structure:
            self.structure.pop()

    def get_test_folder(self, record):
        if record['project_name'] is None:
            return os.path.join(self.rootPath, record['dev_name'], record['start_time'])
        return os.path.join(self.rootPath, record['project_name'], record['dev_name'], record['start_time'])
    
    def get_record_by_tr_name(self, tr_name):
        for record in self.structure:
            if record['tr_name'] == tr_name:
                return record
        self.logger.warning(f"No related test record for {tr_name}")
        return None
   
    def load_test_folder(self, uuid):
        for record in self.structure:
            if record['uuid'] == uuid:
                return self.get_test_folder(record)
        self.logger.warning(f"No related test record for {uuid}")
        return None

    def get_project_folder(self, record):
        return os.path.join(self.rootPath, record['project_name'])

    def load_records(self):
        return self.structure

    def load_uuid(self):
        return {record['uuid'] for record in self.structure}
    
    def load_test_folders(self):
        test_folders = []
        for record in self.structure:
            test_folders.append(self.get_test_folder(record))
        return test_folders
    
    def load_uuid_to_last_dp_timestamp(self):
        return {record['uuid']: record['last_dp_timestamp'] for record in self.structure}

    def load_uuid_to_tr_df_cs_path(self):
        return {record['uuid']: (self.get_tr_path(self.get_test_folder(record)), 
                                 self.get_df_path(self.get_test_folder(record)),
                                 self.get_cycle_stats_path(self.get_test_folder(record))) for record in self.structure}
    
    def load_dev_folder(self, dev_name):
        for record in self.structure:
            if record['dev_name'] == dev_name:
                return os.path.join(self.rootPath, record['project_name'], record['dev_name'])
        self.logger.warning(f"No related test record for {dev_name}")
        return None
    
    def load_processed_dev_folder(self, dev_name):
        for record in self.structure:
            if record['dev_name'] == dev_name:
                return os.path.join(self.rootPath, 'PROCESSED', record['project_name'], record['dev_name'])
        self.logger.warning(f"No processed test record for {dev_name}")
        return None
    
    def load_processed_folder(self):
        return os.path.join(self.rootPath, 'PROCESSED')
    
    def load_ccm_folder(self):
        return os.path.join(self.load_processed_folder(), 'CCM')
    
    def get_tr_path(self, test_folder):
        return os.path.join(test_folder, 'tr.pkl.gz')
    
    def get_df_path(self, test_folder):
        return os.path.join(test_folder, 'df.pkl.gz')
    
    def get_cycle_stats_path(self, test_folder):
        return os.path.join(test_folder, 'cycle_stats.pkl.gz')
    
    def delete_record(self, uuid=None, test_folder=None):
        # Filter out records based on provided uuid or test_folder
        if uuid:
            self.structure = [record for record in self.structure if record['uuid'] != uuid]
        elif test_folder:
            self.structure = [record for record in self.structure if self.get_test_folder(record) != test_folder]
        self._save(self.dirStructurePath, self.structure)

    def update_project_devices(self, devices_id, devices_name, projects_name):
        self.logger.info(f"Updating project devices for {len(devices_name)} devices")
        proj_to_dev_id_name = {}
        for i in range(len(devices_name)):
            if projects_name[i] not in proj_to_dev_id_name:
                proj_to_dev_id_name[projects_name[i]] = []
            proj_to_dev_id_name[projects_name[i]].append((devices_id[i], devices_name[i]))
        self._save(self.projectDevicesPath, proj_to_dev_id_name)

    def load_project_devices(self):
        return self._load(self.projectDevicesPath)
    
    def project_to_devices_id(self, project_name):
        proj_to_dev_id_name = self.load_project_devices()
        if project_name in proj_to_dev_id_name:
            return [item[0] for item in proj_to_dev_id_name[project_name]]
        return []
    
    def project_to_devices_name(self, project_name):
        proj_to_dev_id_name = self.load_project_devices()
        if project_name in proj_to_dev_id_name:
            return [item[1] for item in proj_to_dev_id_name[project_name]]
        return []
    
    def cell_to_project(self, cell_name):
        for record in self.structure:
            if record['dev_name'] == cell_name:
                return record['project_name']
        return None
        