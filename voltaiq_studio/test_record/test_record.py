"""Module for Test Record Related Requests"""
from __future__ import annotations
from typing import List, Generator, Optional
from dataclasses import dataclass, field
from datetime import datetime
import warnings

import pyarrow as pa
import pandas as pd
from ..studio_logger import (
    studio_log,
    log_get_time_series_data,
    log_get_time_series_data_batches,
    log_get_cycle_stats,
    log_get_sweep_stats,
    log_get_cycle_stats_arrow,
    log_get_sweep_stats_arrow,
    log_get_test_records,
    log_get_test_record,
    log_get_test_record_tags,
    log_get_test_record_comments,
    log_get_test_record_trace_keys,
    log_get_all_test_record_comments,
    log_delete_test_record
)

from ..device import Device
from ..session import get_json, put_json, post_json, delete_json
from ..common_types import Comment, get_dataclass_result
from ..filterset import Filterset
from ..flight import (
    TimeSeriesQuery,
    TimeSeriesReader,
    CycleStatQuery,
    CycleStatReader,
    SweepStatQuery,
    SweepStatReader,
    ReadSizeException,
)
from ..flight import cache_management as cm


@dataclass
class PlatformData:
    """Dataclass for TestRecord Meta Data"""

    id: int
    key: str
    value: str
    test_record: int


@dataclass
class TestRecordComment(Comment):
    """Dataclass for Test Record Comment"""

    def update(self, comment: str):
        return update_comment(self.uuid, comment)

    def delete(self):
        return delete_comment(self.uuid)


@dataclass
class TestRecord:
    """Dataclass Representing TestRecord"""

    id: int
    uuid: str
    name: str
    device_id: int
    start_time: Optional[datetime]
    first_dp_timestamp: int
    last_dp_timestamp: int
    total_cycles: int
    status: str
    time_series_query: TimeSeriesQuery = field(init=False)
    cycle_stat_query: CycleStatQuery = field(init=False)
    sweep_stat_query: SweepStatQuery = field(init=False)

    def __post_init__(self):
        """Post Processing to convert ISO String to Datetime"""
        if self.start_time is not None:
            self.start_time = datetime.fromisoformat(self.start_time)
        self.time_series_query = TimeSeriesQuery.for_test_record(self)
        self.cycle_stat_query = CycleStatQuery.for_test_record(self)
        self.sweep_stat_query = SweepStatQuery.for_test_record(self)

    @property
    def tags(self) -> List[str]:
        """Test Record's Tags

        Returns
        -------
        List[str]
            List of Tags for the Test Record
        """
        return get_test_record_tags(self.id)

    def add_tags(self, *args):
        """Add Tags to Test Record's Tags.

        Parameters
        ----------
        *args: str, List[str]
            can be single/multiple args in str, or a list of str

        Returns
        -------
        List[str]
            List of Tags for Test Record
        """
        _tags = self.tags
        for arg in args:
            _tags.extend(arg) if isinstance(arg, list) else _tags.append(arg)
        return update_test_tags(self.id, list(set(_tags)))

    def update_tag(self, old_tag, new_tag):
        """Update an Existing Tag on Test Record.

        Parameters
        ----------
        old_tag: str
            existing tag to be updated
        new_tag: str
            new tag replacing the existing tag

        Returns
        -------
        List[str]
            List of Tags for Test Record
        """
        _tags = self.tags
        try:
            ind = _tags.index(old_tag)
        except ValueError:
            return _tags
        _tags[ind] = new_tag
        return update_test_tags(self.id, _tags)

    def delete_tags(self, *args):
        """Delete Tag(s) on Test Record.

        Parameters
        ----------
        same parameters as add_tags()

        Returns
        -------
        List[str]
            List of Tags for Test Record
        """
        _tags = self.tags
        for arg in args:
            if isinstance(arg, list):
                for tag in arg:
                    try:
                        _tags.remove(tag)
                    except ValueError:
                        continue
            else:
                try:
                    _tags.remove(arg)
                except ValueError:
                    continue
        return update_test_tags(self.id, _tags)

    def update_name(self, name: str):
        """Change Test Record's name"""
        return change_test_name(self.id, name)

    def add_comment(self, comment):
        """Add a Comment to the Test Record"""
        return create_test_comment(self.uuid, comment)

    @property
    def comments(self) -> List[Comment]:
        """Test Record's Comments

        Returns
        -------
        List[Comment]
            List of Comments for the Test Record
        """
        return get_test_record_comments(self.id)

    @property
    def trace_keys(self) -> List[str]:
        """Test Record's Trace Keys

        Returns
        -------
        List[str]
            Trace Keys for the Test Record
        """
        return get_test_record_trace_keys(self.id)

    @property
    def platform_data(self) -> List[PlatformData]:
        """Test Record's Platform Data/Metadata

        Returns
        -------
        List[str]
            Platform Data for the Test Record
        """
        return get_test_record_platform_data(self.id)

    def make_time_series_reader(self) -> TimeSeriesReader:
        """Get a time series reader for this test record

        Returns
        -------
        TimeSeriesReader
            A TimeSeriesReader for this test record.
        """
        try:
            return TimeSeriesReader(self)
        except (IndexError, RuntimeError):
            cm.delete_entry(self)
            return TimeSeriesReader(self)

    @studio_log(log_get_time_series_data)
    def get_time_series_data(self) -> pd.DataFrame:
        """Obtain time series data for this test record's query

        Returns
        -------
        pd.DataFrame
            DataFrame constructed from the requested data
        """
        try:
            return self.make_time_series_reader().read_pandas()
        except ReadSizeException as e:
            raise ValueError(
                "Reader is too large to read full dataframe. Try adding more filtering,\
                     selecting less columns, or using `TestRecord.get_time_series_data_batches()`"
            ) from e

    def get_time_series_data_arrow(self) -> pa.Table:
        """Obtain time series data for this test record's query

        Returns
        -------
        pa.Table
            PyArrow table constructed from the requested data
        """
        try:
            return self.make_time_series_reader().read_arrow()
        except ReadSizeException as e:
            raise ValueError(
                "Reader is too large to read full dataframe. Try adding more filtering,\
                     selecting less columns, or using `TestRecord.get_time_series_data_batches()`"
            ) from e

    @studio_log(log_get_time_series_data_batches)
    def get_time_series_data_batches(self) -> Generator[pd.DataFrame, None, None]:
        """Get a generator of dataframes

        Returns
        -------
        Generator[pd.DataFrame, None, None]
            DataFrames that are batches of this test record's time series data
        """
        return self.make_time_series_reader().read_pandas_batches()

    def reset_query(self) -> None:
        """Reset the Test Record's Query"""
        self.time_series_query = TimeSeriesQuery(id=self.id)

    def make_cycle_stat_reader(self, columns: Optional[List[str]] = []) -> CycleStatReader:
        """Get a cycle statistics reader for this test record

        Parameters
        ----------
        columns : List
            Cycle stat columns to include

        Returns
        -------
        CycleStatReader
            A CycleStatReader for this test record
        """
        self.cycle_stat_query = CycleStatQuery.for_test_record(self)
        self.cycle_stat_query._add_columns(columns)
        return CycleStatReader(self)

    def make_sweep_stat_reader(self, columns: Optional[List[str]] = []) -> SweepStatReader:
        """Get a sweep statistics reader for this test record

        Parameters
        ----------
        columns : List
            Sweep stat columns to include

        Returns
        -------
        SweepStatReader
            A SweepStatReader for this test record
        """
        self.sweep_stat_query = SweepStatQuery.for_test_record(self)
        self.sweep_stat_query._add_columns(columns)
        return SweepStatReader(self)

    @studio_log(log_get_cycle_stats)
    def get_cycle_stats(self, columns: Optional[List[str]] = []) -> pd.DataFrame:
        """Get Cycle Stats Dataframe for this test record

        Parameters
        ----------
        columns : List
            Cycle stat columns to include

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the cycle stats for this test record
        """
        return self.make_cycle_stat_reader(columns).read_pandas()

    @studio_log(log_get_sweep_stats)
    def get_sweep_stats(self, columns: Optional[List[str]] = []) -> pd.DataFrame:
        """Get Sweep Stats Dataframe for this test record

        Parameters
        ----------
        columns : List
            Sweep stat columns to include

        Returns
        -------
        pd.DataFrame
            A DataFrame containing the sweep stats for this test record
        """
        return self.make_sweep_stat_reader(columns).read_pandas()

    def clear_cache(self) -> None:
        """Delete this Test Record's Cached Time Series Data"""
        cm.delete_entry(self)

    @studio_log(log_get_cycle_stats_arrow)
    def get_cycle_stats_arrow(self, columns: Optional[List[str]] = []) -> pd.DataFrame:
        """Get Cycle Stats Dataframe for this test record

        Parameters
        ----------
        columns : List
            Cycle stat columns to include

        Returns
        -------
        pd.DataFrame

        """
        return self.make_cycle_stat_reader(columns).read_arrow()


    @studio_log(log_get_sweep_stats_arrow)
    def get_sweep_stats_arrow(self, columns: Optional[List[str]] = []) -> pd.DataFrame:
        """Get Sweep Stats Dataframe for this test record

        Parameters
        ----------
        columns : List
            Sweep stat columns to include

        Returns
        -------
        pd.DataFrame

        """
        return self.make_sweep_stat_reader(columns).read_arrow()


@studio_log(log_get_test_records)
def get_test_records(filters: dict = None, filter_set: Filterset = None) -> List[TestRecord]:
    """Get all TestRecords

    Parameters
    ----------
    filters : dict
        Key-Value Pairs of Metadata to Filter TestRecords

    Returns
    -------
    List[TestRecord]
        List of TestRecords
    """
    if filters:
        query_str = ""
        warnings.warn(
            "Dictionary filter is deprecated, please use Filterset object", DeprecationWarning
        )
        for k, v in filters.items():
            query_str += f"metadata_key={k}&{k}={v}&"
        response = get_json("test_record?" + query_str)
    elif filter_set:
        query_str = filter_set.parse_filter()
        response = get_json("test_record?" + query_str)
    else:
        response = get_json("test_record/")
    return get_dataclass_result(TestRecord, response)


@studio_log(log_get_test_record)
def get_test_record(id: int) -> TestRecord:
    """Get TestRecord by ID

    Parameters
    ----------
    id : int
        ID of the TestRecord to Get

    Returns
    -------
    TestRecord
        TestRecord matching ID
    """
    response = get_json(f"test_record/{id}/")
    return get_dataclass_result(TestRecord, response)


@studio_log(log_delete_test_record)
def delete_test_record(id: int):
    """Delete TestRecord by ID

    Parameters
    ----------
    id : int
        ID of the TestRecord to Delete
    """
    return delete_json(f"test_record/{id}/")



def change_test_name(id: int, name: str) -> Device:
    """Change Test Record's Name"""
    data = {"name": name}
    response = put_json(f"test_record/{id}/", data)
    return get_dataclass_result(TestRecord, response)


@studio_log(lambda *args, **kwargs: "Called get_all_tags()")
def get_all_tags() -> List[str]:
    """Get all Test Record Tags

    Returns
    -------
    List[str]
        List of Tags
    """
    return get_json("test_record/all_tags/")  # type: ignore


@studio_log(log_get_test_record_tags)
def get_test_record_tags(id: int) -> List[str]:
    """Get a TestRecord's Tags

    Parameters
    ----------
    id : int
        ID of the TestRecord to get Tags for

    Returns
    -------
    List[str]
        List of Tags
    """
    return get_json(f"test_record/{id}/tags/")  # type: ignore


@studio_log(log_get_test_record_comments)
def get_test_record_comments(id: int) -> List[Comment]:
    """Get a TestRecord's Comments

    Parameters
    ----------
    id : int
        ID of the TestRecord to get Comments for

    Returns
    -------
    List[Comment]
        List of Comments
    """
    response = get_json(f"test_record/{id}/comments/")
    return get_dataclass_result(TestRecordComment, response)


@studio_log(log_get_all_test_record_comments)
def get_all_test_record_comments(filter_set: Filterset = None) -> List[Comment]:
    """Get all TestRecords' Comments
    Pass in Filterset to filter through all comments.
    If no filters passed in, all comments will be returned.

    Parameters
    ----------
    filter_set: Filterset
        Filterset object that contains filters for Test Record Comments

    Returns
    -------
    List[Comment]
        List of Comments
    """
    if filter_set:
        query_str = filter_set.parse_filter(filter_obj="comment")
        response = get_json("test_record_comment?" + query_str)
    else:
        response = get_json(f"test_record_comment/")
    return get_dataclass_result(TestRecordComment, response)


@studio_log(log_get_test_record_trace_keys)
def get_test_record_trace_keys(id: int) -> List[str]:
    """Get a TestRecord's Trace Keys

    Parameters
    ----------
    id : int
        ID of the TestRecord to get TraceKeys for

    Returns
    -------
    List[str]
        List of strings
    """
    return get_json(f"test_record/{id}/trace_keys/")  # type: ignore


def get_test_record_platform_data(id: int) -> List[PlatformData]:
    """Get a TestRecord's Platform Data

    Parameters
    ----------
    id : int
        ID of the TestRecord to get Platform Data for

    Returns
    -------
    List[PlatformData]
        List of Platform Data
    """
    response = get_json(f"test_record/{id}/platform_data/")
    return get_dataclass_result(PlatformData, response)


def create_test_comment(uuid: str, comment: str):
    """Create a New Comment for a Test Record

    Parameters
    ----------
    uuid : str
        Test Record UUID to reference comments for
    comment: str
        Comment to Add

    Returns
    -------
    List[Comment]
        List of Comments
    """
    data = {"test_record_uuid": uuid, "comment": comment}
    response = post_json("test_record_comment/", data)
    return get_dataclass_result(TestRecordComment, response)


def update_comment(uuid: str, comment: str):
    """Update a Comment for a Test Record

    Parameters
    ----------
    uuid : str
        Comment UUID to reference comments for
    comment: str
        New Comment

    Returns
    -------
    List[Comment]
        List of Comments
    """
    data = {"comment": comment}
    response = put_json(f"test_record_comment/{uuid}/", data)
    return get_dataclass_result(TestRecordComment, response)


def delete_comment(uuid: str):
    """Delete Comment of Given UUID"""
    return delete_json(f"test_record_comment/{uuid}/")


def update_test_tags(id: int, tags: List[str]):
    """Update an Existing Tag on Device.

    Parameters
    ----------
    tags: List[str]
        List of Tags to Update

    Returns
    -------
    List[str]
        List of Tags for Device
    """
    data = {"tags": tags}
    response = put_json(f"test_record/{id}/tags/", data)
