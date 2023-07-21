"""Module for Managing Cached Time Series Data"""
from __future__ import annotations
import typing as t
from pathlib import Path
from datetime import datetime
from shutil import rmtree

import voltaiq_studio as vs
from voltaiq_studio.global_config import CACHE_PATH
from voltaiq_studio import test_record


if t.TYPE_CHECKING:
    PATHABLE = t.Union[test_record.TestRecord, str, Path]


def __get_cache_object_directory(pathable: PATHABLE) -> Path:
    """Get the Cache Directory for this pathable

    Parameters
    ----------
    pathable : PATHABLE
        Entity which cache path can be searched for

    Returns
    -------
    Path
        Cache directory for the given pathable

    Raises
    ------
    ValueError
        If there is no cache path for this entity within the main cache direcotry
    """
    if isinstance(pathable, Path):
        if all([pathable.parent == CACHE_PATH, pathable.is_dir(), pathable.exists()]):
            return pathable
        raise ValueError(
            f"Path given does not exists or is not a child of the Cache Path: {pathable}"
        )
    if isinstance(pathable, test_record.TestRecord):
        pathable = pathable.uuid
    path = CACHE_PATH / pathable
    if path.is_dir() and path.exists():
        return path
    raise ValueError(f"No cache directory found for test record with uuid: {pathable}")


def __get_cached_file_paths(pathable: PATHABLE) -> t.List[Path]:
    """Get the Paths in the pathable that are files

    Parameters
    ----------
    pathable : PATHABLE
        Entity which cache path can be found for

    Returns
    -------
    t.List[Path]
        List of Paths which are in the cache path
    """
    _dir = __get_cache_object_directory(pathable)
    return [p for p in _dir.iterdir() if p.is_file()]


def _cached_paths() -> t.List[Path]:
    """Get List of Paths that are cached (one path for every file)

    Returns
    -------
    t.List[Path]
        List of Paths Present in the Cache
    """
    return [c for c in CACHE_PATH.iterdir()]


def get_cached_trs() -> t.List[test_record.TestRecord]:
    """Get the Test Records that have cached time series data

    Returns
    -------
    t.List[test_record.TestRecord]
        List of cached test records
    """
    # TODO Replace with a Query using the UUIDs rather than getting all and filtering
    trs = vs.get_test_records()
    uuids = [p.parts[-1] for p in _cached_paths()]
    return [t for t in trs if t.uuid in uuids]


def get_oldest_last_accessed_datetime(pathable: PATHABLE) -> datetime:
    """Get the oldest last accessed datetime for this pathable. It will get the latest of all the
    files in the pathable's cache directory

    Parameters
    ----------
    pathable : PATHABLE
        Entity which cache path can be found for

    Returns
    -------
    datetime
        The last of all file's last accessed

    Raises
    ------
    ValueError
        If there are no cached files for this pathable
    """
    files = __get_cached_file_paths(pathable)
    if len(files) == 0:
        raise ValueError(f"Pathable: {pathable} has no cached files")
    oldest_dt = datetime.utcfromtimestamp(files[0].stat().st_atime)
    for file in files[1:]:
        d_t = datetime.utcfromtimestamp(file.stat().st_atime)
        if d_t < oldest_dt:
            oldest_dt = d_t
    return oldest_dt


def get_oldest_last_modified_datetime(pathable: PATHABLE) -> datetime:
    """Get the last modified datetime for this pathable. It will get the latest of all the files
    in the pathable's cache directory

    Parameters
    ----------
    pathable : PATHABLE
        Entity which cache path can be found for

    Returns
    -------
    datetime
        The last of all file's last modified

    Raises
    ------
    ValueError
        If there are no cached files for this pathable
    """
    files = __get_cached_file_paths(pathable)
    if len(files) == 0:
        raise ValueError(f"Pathable: {pathable} has no cached files")
    oldest_dt = datetime.utcfromtimestamp(files[0].stat().st_mtime)
    for file in files[1:]:
        d_t = datetime.utcfromtimestamp(file.stat().st_mtime)
        if d_t < oldest_dt:
            oldest_dt = d_t
    return oldest_dt


def get_cache_last_modified() -> t.Dict[str, datetime]:
    """Get Dictionary of test_record.TestRecord UUID and their last modified cache time

    Returns
    -------
    t.Dict[str, datetime]
        Dict[uuid of test record, last modified cache]
    """
    paths = _cached_paths()
    l_m = {}
    for p in paths:
        try:
            l_m[p.parts[-1]] = get_oldest_last_modified_datetime(p)
        except ValueError:
            pass
    return l_m


def get_cache_last_accessed():
    """Get Dictionary of test_record.TestRecord UUID and their last accessed cache time

    Returns
    -------
    t.Dict[str, datetime]
        Dict[uuid of test record, last accessed cache]
    """
    paths = _cached_paths()
    l_a = {}
    for p in paths:
        try:
            l_a[p.parts[-1]] = get_oldest_last_accessed_datetime(p)
        except ValueError:
            pass
    return l_a


def delete_entry(pathable: PATHABLE):
    """Delete the Cached entires for this pathable

    Parameters
    ----------
    pathable : PATHABLE
        Entity which cache path can be found for to be delete
    """
    try:
        _dir = __get_cache_object_directory(pathable)
    except ValueError:
        pass
    else:
        rmtree(_dir)


def delete_last_modified_before(d_t: datetime):
    """Delete all Cached entities who were last modified prior to the provided datetime

    Parameters
    ----------
    d_t : datetime
        DateTime to remove entries not modified since before
    """
    l_m = get_cache_last_modified()
    for uuid, modified in l_m.items():
        if modified < d_t:
            delete_entry(uuid)


def delete_last_accessed_before(d_t: datetime):
    """Delete all Cached entities who were last accessed prior to the provided datetime

    Parameters
    ----------
    d_t : datetime
        DateTime to remove entries not accessed since before
    """

    l_a = get_cache_last_accessed()
    for uuid, modified in l_a.items():
        if modified < d_t:
            delete_entry(uuid)
