"""Module for Flight Caching Layer"""
from __future__ import annotations
from typing import TYPE_CHECKING, Optional, Generator, Dict, List, Any, Tuple, Union, Iterable
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as cp
import pyarrow.dataset as ds

from ..request import fetch_flight
from .. import dataset_helpers as dh
from ..time_series.query import (
    CycleRange,
    TimeSeriesQuery,
    TraceFilterOperation,
    TimeSeriesFilterable,
)
from ...exception import VoltaiqStudioException
import voltaiq_studio.global_config as GLOBAL_CONFIG
from ...session import get_json

from ...studio_logger import studio_log, log_add_trace_keys, log_add_info_keys

if TYPE_CHECKING:
    from pyarrow.flight import FlightStreamReader
    import pandas as pd
    from ...test_record import TestRecord


# Columns Used to Index data points
DATAPOINT_INDEX_COL = "h_datapoint_num"


class ReadSizeException(VoltaiqStudioException):
    def __init__(self, num_rows: int, num_columns: int) -> None:
        msg = f"Too many data points, either add filtering or restrict columns. You've requested\
            {num_rows} rows and {num_columns} columns ({num_rows * num_columns} datapoints). Max\
                allowed is {GLOBAL_CONFIG.MAX_DATAPOINT_READ}"
        super().__init__(msg)


class CacheMissingException(VoltaiqStudioException):
    def __init__(self, uuid: str) -> None:
        msg = f"No cache entry found for uuid: {uuid}, did you delete it? Recreate reader and then proceed"
        super().__init__(msg)


class TimeSeriesReader(TimeSeriesFilterable):
    """TimerSeriesReader obtains Flight Data Streams and provides a caching system
    to allow for fastest possible read of data
    """

    def __init__(
        self, test_record: Optional[TestRecord] = None, id: Optional[int] = None
    ) -> None:
        """Create a Reader for the TestRecord. Get the data for the query and cache it.

        Parameters
        ----------
        test_record : TestRecord
            TestRecord to source Query information from
        """
        if test_record is not None:
            self.__query = test_record.time_series_query
            self.__test_id = test_record.id
        elif id is not None:
            self.__query = TimeSeriesQuery(id=id)
            self.__test_id = id
        self.__max_datapoint: Optional[int] = None

        if not GLOBAL_CONFIG.CACHE_PATH.exists():
            GLOBAL_CONFIG.CACHE_PATH.mkdir(parents=True)
        self._cache_dir = GLOBAL_CONFIG.CACHE_PATH / test_record.uuid

        self.__query_and_merge()
        self.__scanner = self.__make_scanner(self.dataset, self.__query)

    @property
    def _query(self) -> TimeSeriesQuery:
        return self.__query

    @_query.setter
    def _query(self, query: TimeSeriesQuery):
        """Update the Reader's Query, request new data if necessary, and re-build scanner

        Parameters
        ----------
        query : TimeSeriesQuery
            Query to update with
        """
        if self.dataset is None:
            raise CacheMissingException(self._cache_dir.parts[-1])
        self.__query = query
        missing_trace, missing_info = self._missing_columns
        if len(missing_trace + missing_info) > 0:
            self.__query_and_merge()
        self.__scanner = self.__make_scanner(self.dataset, self.__query)

    @property
    def dataset(self) -> Optional[ds.Dataset]:
        """Get the Dataset for this reader

        Returns
        -------
        Optional[ds.Dataset]
            Cached dataset for this reader

        :meta private:
        """
        try:
            return ds.parquet_dataset(self.__get_path(GLOBAL_CONFIG.META_DATA_FILE_NAME))
        except FileNotFoundError:
            return None

    @property
    def _schema(self) -> Optional[pa.Schema]:
        """Get the Schema for the Cached Dataset if it exists"""
        try:
            return self.dataset.schema  # type: ignore
        except AttributeError:
            return None

    @property
    def _missing_columns(self) -> Tuple[List[str], List[str]]:
        """Get the Missing columns for this query.

        Returns
        -------
        Tuple[List[str], List[str]]
            Tuple of length 2
            Index 0 is missing trace keys
            Index 1 is missing info keys
        """
        if self._schema is None:
            return ([], [])
        cached_cols = self._schema.names
        return [c for c in self._query.trace_keys if c not in cached_cols], [
            c for c in self._query.info_keys if c not in cached_cols
        ]

    @property
    def _max_datapoint(self) -> Optional[int]:
        """Find the max datapoint that is currently cached if available

        Returns
        -------
        Optional[int]
            Max `DATAPOINT_INDEX_COL` value
        """
        if self.__max_datapoint is None:
            dataset = self.dataset
            if dataset is None or dataset.count_rows() == 0:
                return None
            scanner = dataset.scanner(columns=[DATAPOINT_INDEX_COL])
            self.__max_datapoint = (
                cp.min_max(scanner.to_table().column(DATAPOINT_INDEX_COL)).get("max").as_py()
            )
        return self.__max_datapoint

    @property
    def num_rows(self) -> int:
        """Number of Rows in the Query's Results"""
        return self.__scanner.count_rows()

    @property
    def num_columns(self) -> int:
        """Number of Columns in the Query's Results"""
        return len(self.__scanner.projected_schema.names)

    @property
    def num_datapoints(self) -> int:
        """Number of Datapoints in the Query's Results"""
        return self.num_rows * self.num_columns

    def __get_path(self, filename: str) -> Path:
        return self._cache_dir / filename

    def __get_for_new_columns(self) -> Optional[FlightStreamReader]:
        """Get a FlightStreamReader for all un-cached columns

        Returns
        -------
        Optional[FlightStreamReader]
            FlightStreamReader with new columns, if there are missing columns, else None
        """
        missing_trace, missing_info = self._missing_columns
        if len(missing_trace) == 0 and len(missing_info) == 0:
            return None
        if DATAPOINT_INDEX_COL not in missing_trace:
            missing_trace = [DATAPOINT_INDEX_COL, *missing_trace]
        new_cols_query = TimeSeriesQuery(
            id=self._query.id,
            trace_keys=missing_trace,
            info_keys=missing_info,
        )
        new_cols_query.filter_trace(
            DATAPOINT_INDEX_COL,
            TraceFilterOperation.LESS_THAN_OR_EQUAL,
            self._max_datapoint,
        )
        return fetch_flight(new_cols_query)

    def __get_for_new_datapoints(self) -> FlightStreamReader:
        """Get a FlightStreamReader for any updated data for the columns currently cached

        Returns
        -------
        FlightStreamReader
            Reader with any updated data for the currently cached columns
        """
        schema = self._schema
        cached_cols = schema.names if schema is not None else []
        traces = list(
            set(self._query.trace_keys + [c for c in cached_cols if not c.startswith("i_")])
        )
        if DATAPOINT_INDEX_COL not in traces:
            traces = [DATAPOINT_INDEX_COL, *traces]
        info = list(set(self._query.info_keys + [c for c in cached_cols if c.startswith("i_")]))
        new_data_query = TimeSeriesQuery(
            id=self._query.id,
            trace_keys=traces,
            info_keys=info,
        )
        if self._max_datapoint is not None:
            new_data_query.filter_trace(
                DATAPOINT_INDEX_COL,
                TraceFilterOperation.GREATER_THAN,
                self._max_datapoint,
            )
        return fetch_flight(new_data_query)

    def __query_and_merge(self) -> None:
        """Query for new columns and new data points (h_datapoint_num > current max).
        Merge the results into a single datset
        """
        dataset = self.dataset
        new_datapoints_reader = self.__get_for_new_datapoints()
        # If there is a data set, look for new columns and merge them into the data set
        # Then look for new rows and append them onto the data set
        if dataset is not None:
            cols_reader = self.__get_for_new_columns()
            if cols_reader is not None and len(dataset.files) > 0:
                dh.merge_reader(dataset, cols_reader)
            dh.append_reader(dataset, new_datapoints_reader)
        # If there is no data set, then the new rows are the full data set,
        # Write them to a new data set
        else:
            dh.write_new_dataset(self._cache_dir, new_datapoints_reader)

    @staticmethod
    def __make_scanner(
        dataset: ds.Dataset,
        query: TimeSeriesQuery,
        batch_size: Optional[int] = int(1e6),
    ) -> ds.Scanner:
        """Get a Dataset Scanner for the provided dataset, query, and batchsize

        Parameters
        ----------
        dataset : ds.Dataset
            Dataset to scan
        query : TimeSeriesQuery
            Query to use to select columns and apply filters
        batch_size : Optional[int], optional
            Batch size to have the scanner use, by default int(1e6) (1 Million)

        Returns
        -------
        ds.Scanner
            A Scanner of the Results of the dataset with applied selections and filters
        """
        exp = TimeSeriesReader.__ts_query_to_ds_expression(query)
        columns = query.trace_keys + query.info_keys
        return dataset.scanner(columns=columns, filter=exp, batch_size=batch_size)

    @staticmethod
    def __ts_query_to_ds_expression(
        query: TimeSeriesQuery,
    ) -> Optional[ds.Expression]:
        """Generate a Dataset Filtering Expression from the provided Time Series Query

        Parameters
        ----------
        query : TimeSeriesQuery
            TimeSeriesQuery to filter with

        Returns
        -------
        Optional[ds.Expression]
            Dataset Expression if there is any filtering of the dataset required
        """
        trace_filters = [q.ds_expression for q in query.trace_filters]
        exp = trace_filters[0] if len(trace_filters) > 0 else None
        for tf in trace_filters[1:]:
            exp = exp & tf
        cycles = set(
            query.cycle_numbers + list(CycleRange.get_all_cycle_indexes(query.cycle_ranges))
        )
        if len(cycles) > 0:
            cyc_exp = ds.field("i_cycle_num").isin(cycles)
            exp = (cyc_exp & exp) if exp is not None else cyc_exp
        return exp

    def read_pandas_batches(
        self, batch_size: Optional[int] = None
    ) -> Generator[pd.DataFrame, None, None]:
        """Get a Generator of dataframes for the underlying data

        Parameters
        ----------
        batch_size : Optional[None]
            Batch Size to read, if none is provided, the max batch size will be used

        Returns
        -------
        Generator[pd.DataFrame, None, None]
            Generator of DataFrames

        Raises
        ------
        ReadSizeException
            If Batch size * num columns exceeds the supported read size
        """
        if batch_size is None:
            batch_size = int(GLOBAL_CONFIG.MAX_DATAPOINT_READ / self.num_columns)
        if batch_size * self.num_columns > GLOBAL_CONFIG.MAX_DATAPOINT_READ:
            raise ReadSizeException(batch_size, self.num_columns)
        if self.dataset is None:
            raise CacheMissingException(self._cache_dir.parts[-1])
        return (
            b.to_pandas()
            for b in self.__make_scanner(self.dataset, self._query, batch_size).to_batches()
        )

    def read_pandas(self) -> pd.DataFrame:
        """Get Pandas Data Frame of cached and fetched results

        Returns
        -------
        pd.DataFrame
            DataFrame of interest
        """
        return self.read_arrow().to_pandas()

    def read_arrow(self) -> pa.Table:
        """Get a PyArrow table of cached and fetched results

        Returns
        -------
        Requested table
        """
        if self.num_datapoints > GLOBAL_CONFIG.MAX_DATAPOINT_READ:
            raise ReadSizeException(self.num_rows, self.num_columns)
        try:
            return self.__scanner.to_table()
        except FileNotFoundError:
            raise CacheMissingException(self._cache_dir.parts[-1])


    @studio_log(log_add_trace_keys)
    def add_trace_keys(self, *trace_keys: str) -> None:
        """Update the reader's query with new trace keys

        Example
        -------
        > reader.add_trace_keys('h_current', 'h_potential')
        """
        query = TimeSeriesQuery(**self._query.to_query())
        query.trace_keys += [k for k in trace_keys if k not in query.trace_keys]
        self._query = query


    @studio_log(log_add_info_keys)
    def add_info_keys(self, *info_keys: str) -> None:
        """Update the reader's query with new info keys

        Example
        -------
        > reader.add_info_keys('i_step_num', 'i_cycle_ord')
        """

        query = TimeSeriesQuery(**self._query.to_query())
        query.info_keys += [k for k in info_keys if k not in query.info_keys]
        self._query = query


    def filter_trace(
        self,
        trace_key: str,
        filter_operation: Union[TraceFilterOperation, str],
        value: Any,
    ) -> None:
        """Update the readers query to apply a trace filter with the provided arguments

        Parameters
        ----------
        trace_key : str
            Trace Key to filter on
        filter_operation : Union[TraceFilterOperation, str]
            TraceFilterOperation
        value : Any
            Value to filter with
        """
        query = TimeSeriesQuery(**self._query.to_query())
        query.filter_trace(trace_key, filter_operation, value)
        self._query = query


    def filter_cycle_list(self, cycles: Iterable[int]) -> None:
        """Update the readers query to apply a cycle list filtering

        Parameters
        ----------
        cycles : Iterable[int]
            List of cycles to included on the filter
        """
        query = TimeSeriesQuery(**self._query.to_query())
        query.filter_cycle_list(cycles)
        self._query = query


    def filter_cycle_range(self, cycle_start: int, cycle_end: int, frequency: int = 1) -> None:
        """Update the readers query to apply a cycle range filter

        Parameters
        ----------
        cycle_start : int
            Start of range
        cycle_end : int
            End of range
        frequency : int, optional
            Frequency to step through the range, by default 1
        """
        query = TimeSeriesQuery(**self._query.to_query())
        query.filter_cycle_range(cycle_start, cycle_end, frequency)
        self._query = query


    def get_units(self) -> Dict[str, str]:
        """Returns the units for the traces in this test record

        Returns
        -------
        Dict[str, str]
                Dict mapping trace keys to units
        """
        return get_json(f"test_record/{self.__test_id}/units/")
