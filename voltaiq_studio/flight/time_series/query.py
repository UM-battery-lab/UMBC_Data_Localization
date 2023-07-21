"""Module for Time Series Query Construction"""
from __future__ import annotations
import typing as t
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod

import pyarrow.dataset as ds

from ..query import FlightQuery, get_query_value
from ..trace_filter import TraceFilter, TraceFilterOperation

if t.TYPE_CHECKING:
    from voltaiq_studio.test_record import TestRecord


class _FindEnum(Enum):
    """Enum Base Class to provided search interface"""

    @classmethod
    def find_enum(cls, value) -> t.Optional[_FindEnum]:
        """Search Enum for the provided value"""
        if isinstance(value, cls):
            return value
        for f in cls:
            if f.value == value:
                return f
        return None


class StepType(_FindEnum):
    """Enumerate the Available Step Types"""

    CONSTANT_CURRENT_CHARGE = 1
    CONSTANT_VOLTAGE_CHARGE = 2
    REST_STEP_AFTER_CHARGE = 3
    CONSTANT_CURRENT_DISCHARGE = 4
    CONSTANT_VOLTAGE_DISCHARGE = 5
    REST_STEP_AFTER_DISCHARGE = 6
    OTHER_CHARGE = 7
    OTHER_DISCHARGE = 8
    OTHER_REST = 9
    OTHER = 10


class TimeSeriesFilterable(ABC):
    @abstractmethod
    def filter_trace(
        self, trace_key: str, filter_operation: t.Union[TraceFilterOperation, str], value: t.Any
    ) -> None:
        """Add a Trace Filter to this query

        Parameters
        ----------
        trace_key : str
            Trace Key to filter on
        filter_operation : Union[TraceFilterOperation, str]
            The Filter choice
        value : float
            The Value to filter against
        """

    @abstractmethod
    def filter_cycle_list(self, cycles: t.Iterable[int]) -> None:
        """Extend this Query's Cycle numbers

        Parameters
        ----------
        cycles : t.Iterable[int]
            Iterable of integers to add to to cycle ranges
        """

    @abstractmethod
    def filter_cycle_range(self, cycle_start: int, cycle_end: int, frequency: int = 1) -> None:
        """Add a cycle range filter, allowing the user to set start, end, and frequency
        of cycles to include
        Ex:
        ```
        query.filter_cycle_range(10, 100, 5)
        # All multiples of 5 from 10 to 100 would be included in results
        ```
        Parameters
        ----------
        cycle_start : int
            Start Number
        cycle_end : int
            End Cycle Number
        frequency : int, optional
            Frequency to increment at between start and end, by default 1
        """


@dataclass
class CycleRange:
    """Dataclass to trace Cycle Range Filter Logic"""

    start: int
    end: int
    frequency: int = 1

    @staticmethod
    def get_all_cycle_indexes(ranges: t.List[CycleRange]) -> t.Set[int]:
        """Get all the of the cycles which are options in the provided list of cycle ranges

        Parameters
        ----------
        ranges : t.List[CycleRange]
            List of Cycle Ranges to get cycles for

        Returns
        -------
        t.Set[int]
            Set of integer cycle indexes
        """
        cycles = []
        for _range in ranges:
            cycles.extend(list(range(_range.start, _range.end, _range.frequency)))
        return set(cycles)

    def __post_init__(self):
        """Ensure Data types are correct"""
        for key, value in self.__dict__.items():
            setattr(self, key, int(value))

    def to_query(self) -> dict:
        return self.__dict__


@dataclass
class TimeSeriesQuery(FlightQuery, TimeSeriesFilterable):
    """Dataclass For a Time Series Query
    `action` : Implemented for future development, do not change
    `trace_keys` : Column Names of trace data to be returned in the query results
        for example current, potential, etc
    `info_keys` : Column Names of information data to be returned in the query results
        for example data point number, cycle number, etc
    `cycle_numbers` : Selected cycle indexes to retrieve in the query, for example `[1,5,10]`
    `cycle_ranges` : Ranges of cycle indexes to retrieve in the query, for example
        `[{"start": 0, "end": 100, "frequency": 5}]
    NOTE: `cycle_numbers` and `cycle_ranges` and inclusive filters, meaning that the union of all
        entires in these fields will be returned, it is an `OR` query
    `step_indexes` : Step indexes to be returned in each cycle, if not set all included
    `step_types` : The step types to be returned in the query, for example constat current charge
    `trace_filters` : Filters on trace keys. For example filtering where current is greater than 0
    """

    id: int
    action: str = "time_series"
    trace_keys: t.List[str] = field(default_factory=list)
    info_keys: t.List[str] = field(default_factory=list)
    cycle_numbers: t.List[int] = field(default_factory=list)
    cycle_ranges: t.List[CycleRange] = field(default_factory=list)
    step_indexes: t.List[int] = field(default_factory=list)
    step_types: t.List[StepType] = field(default_factory=list)
    trace_filters: t.List[TraceFilter] = field(default_factory=list)

    @classmethod
    def for_test_record(cls, test_record: TestRecord) -> TimeSeriesQuery:
        """Get a new TimeSeriesQuery for a test record

        Parameters
        ----------
        test_record : TestRecord
            TestRecord to get a query

        Returns
        -------
        TimeSeriesQuery
            Query that will get data for a test record
        """
        return cls(id=test_record.id)

    def __post_init__(self):
        """Validation and type setting"""
        self.trace_filters = [TraceFilter.from_dict(d) for d in self.trace_filters]
        self.step_types = [StepType.find_enum(s) for s in self.step_types]
        self.trace_keys = list(set(self.trace_keys))
        self.info_keys = list(set(self.info_keys))

    def to_query(self) -> dict:
        """Get the Time Series formatted for the Flight Server

        Returns
        -------
        str
            Serialized TimeSeries Query
        """
        as_dict = {}
        for key, value in self.__dict__.items():
            if isinstance(value, list):
                data = [get_query_value(v) for v in value]
            else:
                data = get_query_value(value)
            if data:
                as_dict[key] = data
        return as_dict

    def filter_trace(
        self, trace_key: str, filter_operation: t.Union[TraceFilterOperation, str], value: t.Any
    ) -> None:
        """Add a Trace Filter to this query

        Parameters
        ----------
        trace_key : str
            Trace Key to filter on
        filter_operation : Union[TraceFilterOperation, str]
            The Filter choice
        value : float
            The Value to filter against
        """
        _filter_operation: TraceFilterOperation = TraceFilterOperation.find_enum(  # type: ignore
            filter_operation
        )
        if _filter_operation is None:
            raise ValueError(f"Invalid Filter Key Provided: {filter_operation}")
        filter = TraceFilter(trace_key=trace_key, key=_filter_operation, value=value)
        self.trace_filters.append(filter)

    def filter_cycle_list(self, cycles: t.Iterable[int]) -> None:
        """Extend this Query's Cycle numbers

        Parameters
        ----------
        cycles : t.Iterable[int]
            Iterable of integers to add to to cycle ranges
        """
        self.cycle_numbers = list(set(self.cycle_numbers + list(cycles)))

    def filter_cycle_range(self, cycle_start: int, cycle_end: int, frequency: int = 1) -> None:
        """Add a cycle range filter, allowing the user to set start, end, and frequency
        of cycles to include
        Ex:
        ```
        query.filter_cycle_range(10, 100, 5)
        # All multiples of 5 from 10 to 100 would be included in results
        ```
        Parameters
        ----------
        cycle_start : int
            Start Number
        cycle_end : int
            End Cycle Number
        frequency : int, optional
            Frequency to increment at between start and end, by default 1
        """
        c_range = CycleRange(start=cycle_start, end=cycle_end, frequency=frequency)
        self.cycle_ranges.append(c_range)
