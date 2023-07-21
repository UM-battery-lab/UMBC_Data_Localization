"""Default Config Values for Voltaiq Studio Library"""
import os
from pathlib import Path


MAX_DATAPOINT_READ = 1e9  # One billion Datapoints, this is Rows x Columns
META_DATA_FILE_NAME = "_metadata"
PARQUET_EXTENSION = ".pqt"
PARQUET_PARTITION_SIZE = 100000  # Rows per partition

# Fully Qualified path in which to cache data
CACHE_DIR = os.environ.get("FLIGHT_CACHE_DIR", "/tmp/flight")
if CACHE_DIR is None:
    raise EnvironmentError(
        "No Cache Directory set, please set env variable `FLIGHT_CACHE_DIR` with path"
    )
CACHE_PATH = Path(CACHE_DIR)

# FLIGHT RETRY CONFIGS

FLIGHT_RETRY_MIN = 500
FLIGHT_RETRY_MAX = 2000
