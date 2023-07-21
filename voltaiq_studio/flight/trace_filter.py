from __future__ import annotations
from dataclasses import dataclass
import pyarrow.dataset as ds
import typing as t
from enum import Enum


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


class TraceFilterOperation(_FindEnum):
    """Enumerate the Available Trace Filter Keys"""

    GREATER_THAN = "gt"
    GREATER_THAN_OR_EQUAL = "gte"
    LESS_THAN = "lt"
    LESS_THAN_OR_EQUAL = "lte"
    EQUAL = "eq"
    NOT_EQUAL = "neq"
    # Support for commented out filters needs further investigation
    # RANGE = "range"
    # NOT_RANGE = "not_range"
    # LIKE = "like"
    IN = "in"


# Map from TraceFilterOperations to a callable that accepts field and value and returns
# the Expression logic
TRACE_FILTER_TO_DS_MAP: t.Dict[
    TraceFilterOperation, t.Callable[[ds.Expression, t.Any], ds.Expression]
] = {
    TraceFilterOperation.EQUAL: lambda field, v: field == v,
    TraceFilterOperation.NOT_EQUAL: lambda field, v: field != v,
    TraceFilterOperation.GREATER_THAN: lambda field, v: field > v,
    TraceFilterOperation.GREATER_THAN_OR_EQUAL: lambda field, v: field >= v,
    TraceFilterOperation.LESS_THAN: lambda field, v: field < v,
    TraceFilterOperation.LESS_THAN_OR_EQUAL: lambda field, v: field <= v,
    TraceFilterOperation.IN: lambda field, v: field.isin(v),
}


@dataclass
class TraceFilter:
    """Dataclass to track Trace Filter Logic"""

    trace_key: str
    key: TraceFilterOperation
    value: float

    def to_query(self) -> dict:
        """Format the TraceFilter how it is needed to querying API"""
        return {"trace_key": self.trace_key, self.key.value: self.value}

    @property
    def ds_expression(self) -> ds.Expresion:
        """Get a DataSet Expression for this filter

        Returns
        -------
        ds.Expresion
            DataSet Expression for this filter
        """
        field = ds.field(self.trace_key)
        return TRACE_FILTER_TO_DS_MAP[self.key](field, self.value)

    @classmethod
    def from_dict(cls, data: dict) -> TraceFilter:
        """Create a TraceFilter from the provided data"""
        parsed_data = {}
        for key, value in data.items():
            if key == "trace_key":
                parsed_data[key] = value
            else:
                parsed_data["key"] = TraceFilterOperation.find_enum(key)
                parsed_data["value"] = value
        return cls(**parsed_data)
