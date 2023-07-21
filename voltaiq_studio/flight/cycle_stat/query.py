"""Module for Cycle Stat Flight Query Logic"""
from __future__ import annotations

import typing as t
from dataclasses import dataclass, field
from datetime import datetime

from ..query import FlightQuery
from ..time_series.query import TraceFilter, TraceFilterOperation

if t.TYPE_CHECKING:
    from ...test_record import TestRecord


@dataclass
class CycleStatQuery(FlightQuery):
    """Flight Query for Cycle stats"""

    test_record_id: int
    test_record_uuid: str
    action: str = "cycle_stat"
    columns: t.Optional[t.List[str]] = None
    trace_filters: t.List[TraceFilter] = field(default_factory=list)

    @classmethod
    def for_test_record(cls, test_record: TestRecord) -> CycleStatQuery:
        """Create a CycleStatQuery for a TestRecord"""
        return cls(test_record_id=test_record.id, test_record_uuid=test_record.uuid)

    def to_query(self) -> t.Union[str, bytes, dict]:
        """Shape a Query for API Request"""
        return self.__dict__

    def _add_columns(self, columns: t.Union[t.List, None]) -> None:
        """Add columns to filter for."""

        if not isinstance(columns, list):
            raise Exception(f"columns must be a list, not {type(columns)}")
        else:
            if self.columns is None:
                self.columns = columns
            else:
                new_columns = [k for k in columns if k not in self.columns]
                self.columns.extend(new_columns)

    def filter_trace(
        self, trace_key: str, filter_operation: t.Union[TraceFilterOperation, str], value: t.Any
    ) -> None:
        """Add a Trace Filter to CycleStatQuery

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
        if isinstance(value, datetime):
            value = value.isoformat()
        filter = TraceFilter(trace_key=trace_key, key=_filter_operation, value=value).to_query()
        self.trace_filters.append(filter)
