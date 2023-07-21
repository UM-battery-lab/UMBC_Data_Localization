"""Module for Device Related Requests"""
from __future__ import annotations
from typing import Optional, Any, List
from dataclasses import dataclass
from datetime import datetime
import warnings

from .session import get_json, put_json, post_json, delete_json
from .common_types import Comment, get_dataclass_result
from .filterset import Filterset
from .studio_logger import studio_log, log_get_devices, log_get_device, log_get_device_tags, \
    log_get_device_comments, log_get_device_metadata, log_get_all_device_comments

@dataclass
class MetaData:
    """Dataclass for Device Meta Data"""

    id: int
    unit: str
    key: str
    value: str
    h_value: Optional[Any]
    data_type: str
    device: int
    uuid: str

@dataclass
class DeviceComment(Comment):
    """Dataclass for Device Comment"""

    def update(self, comment: str):
        """Update the Given Comment"""
        return update_comment(self.uuid, comment)

    def delete(self):
        """Delete the Given Comment"""
        return delete_comment(self.uuid)

@dataclass
class DeviceAttribute:
    """Dataclass for Device Attribute"""
    id: int
    created: datetime
    updated: datetime
    attribute_key: str
    value: dict
    tags: List[str]
    uuid: str
    device: str

    def __post_init__(self):
        """Post Processing to set ISO Strings to Datetimes
        and create attribute
        """
        self.created = datetime.fromisoformat(self.created)
        self.updated = datetime.fromisoformat(self.updated)
    
    def update(self, value: dict):
        """Update the Given Attribute"""
        result = update_device_attribute_value(
            self.uuid, 
            value, 
            attribute_key=self.attribute_key,
            tags=self.tags,
            device=self.device
            )
        return result

    def delete(self):
        """Delete the Given Attribute"""
        return delete_device_attribute(self.uuid)


@dataclass
class Device:
    """Dataclass for Device"""

    id: int
    name: str
    created: datetime
    updated: datetime
    last_processed: Optional[datetime]
    uuid: str

    def __post_init__(self):
        """Post Processing to set ISO Strings to Datetimes"""
        self.created = datetime.fromisoformat(self.created)
        self.updated = datetime.fromisoformat(self.updated)
        self.last_processed = (
            None if self.last_processed is None else datetime.fromisoformat(self.last_processed)
        )

    @property
    def comments(self) -> List[Comment]:
        """Device's Comments

        Returns
        -------
        List[Comment]
            List of Comments for the Device
        """
        return get_device_comments(self.id)

    @property
    def tags(self) -> List[str]:
        """Device's Tags

        Returns
        -------
        List[str]
            List of Tags for Device
        """
        return get_device_tags(self.id)

    def add_tags(self, *args):
        """Add Tags to Device's Tags.
        
        Parameters
        ----------
        *args: str, List[str]
            can be single/multiple args in str, or a list of str
        
        Returns
        -------
        List[str]
            List of Tags for Device
        """
        _tags = self.tags
        for arg in args:
            _tags.extend(arg) if isinstance(arg, list) else _tags.append(arg)
        return update_device_tags(self.id, list(set(_tags)))
    

    def update_tag(self, old_tag, new_tag):
        """Update an Existing Tag on Device.
        
        Parameters
        ----------
        old_tag: str
            existing tag to be updated
        new_tag: str
            new tag replacing the existing tag
        
        Returns
        -------
        List[str]
            List of Tags for Device
        """
        _tags = self.tags
        try:
            ind = _tags.index(old_tag)
        except ValueError:
            return _tags 
        _tags[ind] = new_tag
        return update_device_tags(self.id, _tags)
    

    def delete_tags(self, *args):
        """Delete Tag(s) on Device.
        
        Parameters
        ----------
        same parameters as add_tags()
        
        Returns
        -------
        List[str]
            List of Tags for Device
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
        return update_device_tags(self.id, _tags)     


    @property
    def meta_data(self) -> List[MetaData]:
        """Device's MetaData

        Returns
        -------
        List[MetaData]
            List of MetaData for Device
        """
        return get_device_metadata(self.id)

    def update_name(self, name: str):
        """Change Device's name"""
        return change_device_name(self.id, name)

    def add_comment(self, comment):
        """Add a Comment to Device """
        return create_device_comment(self.uuid, comment)
    
    def attributes(self):
        """Get Device's Attributes"""
        return get_device_attributes(self.id)
    
    def add_attribute( 
        self,
        attribute_key: str, 
        value: dict, 
        tags: List[str] = [],
        is_new_key: bool  = True,
        is_multi: bool = True
        ):
        """Add an Attribute to Device """
        return create_device_attribute(self.uuid, attribute_key, value, is_new_key, is_multi, tags)

    def update_project(self, project: str):
        """Update the project a device is in.
        
        Parameters
        ----------
        project: str
            Name of the project to assign the device
        
        Returns
        -------
        None
        """
        return update_project(self.id, project)





@studio_log(log_get_devices)
def get_devices(filters: dict = None, filter_set: Filterset = None) -> List[Device]: # EIS devices would show up here
    """Get All Devices

    Parameters
    ----------
    filters : dict
        Key-Value Pairs of Metadata to Filter Devices
    filter_set: Filterset
        Filterset object that contains filters for Devices

    Returns
    -------
    List[Device]
        List of Devices
    """
    if filters:
        query_str = ""
        warnings.warn("Dictionary filter is deprecated, please use Filterset object", DeprecationWarning)
        for k, v in filters.items():
            query_str += f"metadata_key={k}&{k}={v}&"
        response = get_json("device?" + query_str)
    elif filter_set:
        query_str = filter_set.parse_filter(filter_obj="device")
        response = get_json("device?" + query_str)
    else:
        response = get_json("device/")
    return get_dataclass_result(Device, response)


@studio_log(log_get_device)
def get_device(id: int) -> Device:
    """Get Device By ID

    Parameters
    ----------
    id : int
        Device's ID

    Returns
    -------
    Device
        Device Matching ID
    """
    response = get_json(f"device/{id}/")
    return get_dataclass_result(Device, response)  # type: ignore

def change_device_name(id: int, name: str) -> Device:
    """Change Device's Name"""
    data = {
        "name": name
    }
    response = put_json(f"device/{id}/", data)
    return get_dataclass_result(Device, response)


@studio_log(lambda *args, **kwargs: "Called get_all_device_tags()")
def get_all_device_tags() -> List[str]:
    """Get All Devices Tags Available

    Returns
    -------
    List[str]
        List of Tags
    """
    return get_json("device/all_tags/")  # type: ignore


@studio_log(log_get_device_tags)
def get_device_tags(id: int) -> List[str]:
    """Get a Device's Tags

    Parameters
    ----------
    id : int
        Device ID to get Tags for

    Returns
    -------
    List[str]
        List of Strings
    """
    return get_json(f"device/{id}/tags/")  # type: ignore


@studio_log(log_get_device_comments)
def get_device_comments(id: int) -> List[Comment]:
    """Get a Device's Comments

    Parameters
    ----------
    id : int
        Device ID to get Comments for

    Returns
    -------
    List[Comment]
        List of Comments
    """
    response = get_json(f"device/{id}/comments/")
    return get_dataclass_result(DeviceComment, response)

@studio_log(log_get_all_device_comments)
def get_all_device_comments(filter_set: Filterset = None) -> List[Comment]:
    """Get all Devices' Comments
    Pass in Filterset to filter through all comments.
    If no filters passed in, all comments will be returned.

    Parameters
    ----------
    filter_set: Filterset
        Filterset object that contains filters for Device Comments

    Returns
    -------
    List[Comment]
        List of Comments
    """
    if filter_set:
        query_str = filter_set.parse_filter(filter_obj="comment")
        response = get_json("device_comment?" + query_str)
    else:
        response = get_json(f"device_comment/")
    return get_dataclass_result(DeviceComment, response)


@studio_log(log_get_device_metadata)
def get_device_metadata(id: int) -> List[MetaData]:
    """Get a Device's MetaData

    Parameters
    ----------
    id : int
        Device ID to get MetaData for

    Returns
    -------
    List[MetaData]
        List of MetaData
    """
    response = get_json(f"device/{id}/meta_data/")
    return get_dataclass_result(MetaData, response)


def create_device_comment(uuid: str, comment: str):
    """Create a New Comment for a Device

    Parameters
    ----------
    uuid : str
        Device UUID to reference comments for
    comment: str
        Comment to Add

    Returns
    -------
    List[Comment]
        List of Comments
    """
    data = {
        "device_uuid": uuid,
        "comment": comment
    }
    response = post_json("device_comment/", data)
    return get_dataclass_result(DeviceComment, response)


def update_comment(uuid: str, comment: str):
    """Update a Comment for a Device

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
    data = {
        "comment": comment
    }
    response = put_json(f"device_comment/{uuid}/", data)
    return get_dataclass_result(DeviceComment, response)

def delete_comment(uuid: str):
    """Delete Comment of Given UUID"""
    return delete_json(f"device_comment/{uuid}/")

def update_device_tags(id: int, tags: List[str]):
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
    data = {
        "tags": tags 
    }
    response = put_json(f"device/{id}/tags/", data)

def update_project(id: int, project: str):
    """Update a project on a Device.
        
        Parameters
        ----------
        project: str
            Name of the project to assign the device
        
        Returns
        -------
        None
        """
    data = {
        "project": project 
    }
    response = put_json(f"device/{id}/project/", data)

def get_device_attributes(id: int):
    """Get a Device's Attributes

    Parameters
    ----------
    id : int
        Device ID to get Attributes for

    Returns
    -------
    List[Attribute]
        List of Attributes
    """
    response = get_json(f"device/{id}/attributes/")
    return get_dataclass_result(DeviceAttribute, response)

def create_device_attribute(
    device_uuid: str, 
    attribute_key: str, 
    value: dict,
    is_new_key: bool = True,
    is_multi: bool = True,
    tags: list[str] = []
    ):
    """
    Create Attribute for Device
    
    Parameters
    ----------
    device_uuid : str
        Device UUID to reference attribute for

    Returns
    -------
    List[Attibute]
        List of Attributes
    """
    data = {
        "device": device_uuid, 
        "attribute_key": attribute_key, 
        "value": value,
        "is_new_key": is_new_key,
        "is_multi": is_multi,
        "tags": tags
    }
    response = post_json("device_attribute/", data, json_dump=True)
    return get_dataclass_result(DeviceAttribute, response)

def update_device_attribute_value(uuid: str, value: dict, **kwargs):
    """Update a Attribute for a Device

    Parameters
    ----------
    uuid : str
        Attribute UUID to reference attributes for
    value: str
        New value of the attribute

    Returns
    -------
    List[Attribute]
        List of Attributes
    """
    data = {"value": value}
    response = put_json(f"device_attribute/{uuid}/", data, json_dump=True)
    for key, value in kwargs.items():
        response[key] = value
    return get_dataclass_result(DeviceAttribute, response)

def delete_device_attribute(uuid: str):
    """Delete Attribute of Given UUID"""
    return delete_json(f"device_attribute/{uuid}/")
