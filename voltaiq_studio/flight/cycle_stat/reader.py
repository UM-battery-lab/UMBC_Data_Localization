"""Module for Reading Cycle Stat Data from API"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Union

from ...session import get_json
from ..request import fetch_flight

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa
    from ...test_record import TestRecord

from ..time_series.query import TraceFilterOperation

from ...studio_logger import studio_log, log_cycle_stats_filter_trace, log_add_columns


class CycleStatReader:
    """Reader class for Cycle Stat Queries"""

    units_endpoint = "cycle_stats_units"

    def __init__(self, test_record: TestRecord) -> None:
        """Initialize the reader for a test record

        Parameters
        ----------
        test_record : TestRecord
            TestRecord to query for
        """
        self.__query = self.__get_query(test_record)
        self.__test_id = test_record.id

    def __get_query(self, test_record) -> Union[SweepStatReader, CycleStatReader]:
        if "cycle" in self.units_endpoint:
            return test_record.cycle_stat_query
        else:
            return test_record.sweep_stat_query

    def read_pandas(self) -> pd.DataFrame:
        """Get the Query Results as a pandas DataFrame

        Returns
        -------
        pd.DataFrame
            DataFrame of cycle stat results
        """
        return fetch_flight(self.__query).read_pandas()

    def read_arrow(self) -> pa.Table:
        """Get the Query Results as a pyarrow table

        Returns
        -------
        pa.Table
            PyArrow Table of cycle stat results
        """
        return fetch_flight(self.__query).read_all()

    def get_units(self) -> Dict[str, str]:
        """Returns the units for the cycle statistics

        Returns
        -------
        Dict[str, str]
            Dict mapping cycle stat keys to units
        """
        return get_json(f"test_record/{self.__test_id}/{self.units_endpoint}/")

    @studio_log(log_add_columns)
    def add_columns(self, *columns: str) -> None:
        """Update the reader's query with new columns

        Example
        -------
        > reader.add_columns('cycle_number', 'cyc_charge_capacity')
        """
        self.__query._add_columns(list(columns))

    @studio_log(log_cycle_stats_filter_trace)
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
        self.__query.filter_trace(trace_key, filter_operation, value)
