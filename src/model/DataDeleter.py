import os
import shutil
from src.utils.Logger import setup_logger


class DataDeleter:
    """
    The class to delete data from the local disk

    Attributes
    ----------
    logger: logger object
        The object to log information
    
    Methods
    -------
    delete_file(file_path)
        Delete the file with the specified path
    delete_folders(folder_list)
        Delete the specified folders if they are empty or contain only tr.pkl.gz or df.pkl.gz
    """
    def __init__(self):
        self.logger = setup_logger()

    def delete_file(self, file_path):
        try:
            os.remove(file_path)
        except Exception as e:
            self.logger.error(f'Error while deleting file {file_path}: {e}')

    def delete_folders(self, folder_list):
        """
        Delete the specified folders if they are empty or contain only tr.pkl.gz or df.pkl.gz.

        Parameters
        ----------
        folder_list: list of str
            List of folder paths to be deleted.

        Returns
        -------
        None
        """
        for folder in folder_list:
            try:
                # Check folder contents
                current_files = [file.name for file in os.scandir(folder)]
                if set(current_files) == {"tr.pkl.gz", "df.pkl.gz"}:
                    self.logger.warning(f"Folder {folder} contains valid files. Skipping deletion.")
                else:
                    self.logger.info(f"Folder {folder} contains invalid files: {current_files}.")
                    confirmation = input(f"Delete folder {folder}? (y/n): ")
                    if confirmation != "y":
                        continue
                    shutil.rmtree(folder)
                    self.logger.info(f"Deleted folder: {folder}")
            except Exception as e:
                self.logger.error(f"Error deleting folder {folder}: {e}")