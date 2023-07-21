"""Module for Sweep Stat Flight Query Logic"""
from __future__ import annotations

import typing as t
from dataclasses import dataclass, field
from datetime import datetime

from ..query import FlightQuery
from ..time_series.query import TraceFilter, TraceFilterOperation
from ..cycle_stat.query import CycleStatQuery

if t.TYPE_CHECKING:
    from ...test_record import TestRecord


@dataclass
class SweepStatQuery(CycleStatQuery):
    """Flight Query for Sweep stats"""

    test_record_id: int
    test_record_uuid: str
    action: str = "sweep_stat"
    columns: t.Optional[t.List[str]] = None
    trace_filters: t.List[TraceFilter] = field(default_factory=list)
