"""Module for Reading Sweep Stat Data from API"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Union

from ...session import get_json
from ..request import fetch_flight

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow as pa
    from ...test_record import TestRecord

from ..time_series.query import TraceFilterOperation
from ..cycle_stat.reader import CycleStatReader


from ...studio_logger import studio_log, log_add_columns


class SweepStatReader(CycleStatReader):
    """Reader class for Sweep Stat Queries"""

    units_endpoint = "sweep_stats_units"
