"""Module for Abstract Query Functionality"""
from __future__ import annotations
import typing as t
from abc import ABC, abstractmethod


class FlightQuery(ABC):
    @abstractmethod
    def to_query(self) -> t.Union[str, bytes, dict]:
        pass


def get_query_value(item):
    """Inspect the item and obtain the query data"""
    return item.to_query() if hasattr(item, "to_query") else item
