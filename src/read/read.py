import pickle
from logger_config import setup_logger

# Setup logger
logger = setup_logger()


def load_record(file_path):
    # Load the test record from the pickle file
    with open(file_path, "rb") as f:
        record = pickle.load(f)
    logger.info(f"Loaded test record from {file_path}")
    logger.info(f"Record: {record}")
    return record
