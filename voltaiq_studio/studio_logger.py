import logging
import os
from functools import wraps
from typing import List, Any

from .filterset import Filterset

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

studio_username = os.getenv("JUPYTERHUB_USER")
if studio_username:
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        filename=f"/srv/studio-logging/{studio_username}.log",
        level=logging.DEBUG,
    )


def studio_log(process_params):
    def outer(func):
        @wraps(func)
        def inner(*args, **kwargs):
            try:
                params = process_params(*args, **kwargs)
                logger.debug(f"Studio user {studio_username}: {params}")
                return func(*args, **kwargs)

            except Exception as e:
                logger.exception(e)
                raise e

        return inner

    return outer


def log_get_devices(filters: dict = None, filter_set: Filterset = None):
    return f"Called get_devices with filters: {filters}"


def log_get_device(id):
    return f"Called get_device for id: {id}"


def log_get_device_tags(id):
    return f"Called get_device_tags for id: {id}"


def log_get_device_comments(id):
    return f"Called get_device_comments for id: {id}"

def log_get_all_device_comments(filter_set: Filterset = None):
    return f"Called get_all_device_comments for Filters: {filter_set}"

def log_get_device_metadata(id):
    return f"Called get_device_metadata for id: {id}"


def log_get_time_series_data(self):
    return f"Called get_time_series_data for TestRecord id: {self.id}"


def log_get_time_series_data_batches(self):
    return f"Called get_time_series_data_batches for TestRecord id: {self.id}"


def log_get_cycle_stats(self, columns: List = []):
    return f"Called get_cycle_stats for TestRecord id: {self.id} with columns: {columns}"


def log_get_cycle_stats_arrow(self, columns: List = []):
    return f"Called get_cycle_stats_arrow for TestRecord id: {self.id} with columns: {columns}"


def log_cycle_stats_filter_trace(self, trace_key: str, filter_operation: str, value: Any):
    return f"Called filter_trace for {self}: with trace key: {trace_key}, filter_operation: {filter_operation}, value: {value}"


def log_get_sweep_stats(self, columns: List = []):
    return f"Called get_sweep_stats for TestRecord id: {self.id} with columns: {columns}"


def log_get_sweep_stats_arrow(self, columns: List = []):
    return f"Called get_sweep_stats_arrow for TestRecord id: {self.id} with columns: {columns}"


def log_add_columns(*columns: str):
    return f"Called add_columns with columns: {columns}"


def log_get_test_records(filters: dict = None, filter_set: Filterset = None):
    return f"Called get_test_records with filters: {filters}"


def log_get_test_record(id):
    return f"Called get_test_record for id: {id}"


def log_delete_test_record(id):
    return f"Called delete_test_record for id: {id}"


def log_get_test_record_tags(id):
    return f"Called get_test_record_tags for id: {id}"


def log_get_test_record_comments(id):
    return f"Called get_test_record_comments for id: {id}"


def log_get_all_test_record_comments(filter_set: Filterset = None):
    return f"Called get_all_test_record_comments for Filters: {filter_set}"


def log_get_test_record_trace_keys(id):
    return f"Called get_test_record_trace_keys for id: {id}"


def log_add_trace_keys(*trace_keys: str):
    return f"Called add_trace_keys with trace_keys: {trace_keys}"


def log_add_info_keys(*info_keys: str):
    return f"Called add_info_keys with info_keys: {info_keys}"
