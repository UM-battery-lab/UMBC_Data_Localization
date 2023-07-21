"""Module for Shared Data Types"""
from __future__ import annotations
from typing import Union, List, Dict
from dataclasses import dataclass
from datetime import datetime
import dataclasses


@dataclass
class Comment:
    """Dataclass for Comment Model"""

    id: int
    created: datetime
    updated: datetime
    comment: str
    author: str
    uuid: str

    def __post_init__(self):
        """Post Processing to set ISO Strings to Datetimes"""
        self.created = datetime.fromisoformat(self.created)
        self.updated = datetime.fromisoformat(self.updated)

def get_dataclass_result(dataclass_model, response: Union[List[Dict], Dict]) -> Union[List[Dict], Dict]:
    """Filter kwargs by those present in the dataclass' field and return the results
    
    Parameters
    ----------
    dataclass_model:
        Dataclass model (e.g. Device, TestRecord, Comment) 
    response: List[Dict] or Dict
        Dict or list of dicts from the json response

    Returns
    -------
    List[Dict] or Dict
        Containing result(s) from the dataclass

    """
    field_names = set(f.name for f in dataclasses.fields(dataclass_model))
    if isinstance(response, list):
        return [dataclass_model(**{k:v for k,v in d.items() if k in field_names}) for d in response]
    elif isinstance(response, dict):
        return dataclass_model(**{k:v for k,v in response.items() if k in field_names})
