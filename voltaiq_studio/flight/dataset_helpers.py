"""Module for Extending PyArrow Dataset Logic"""
from __future__ import annotations
import typing as t
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from . import table_helpers as th
import voltaiq_studio.global_config as GLOBAL_CONFIG


if t.TYPE_CHECKING:
    import pyarrow.flight as fl  # pylint: disable=ungrouped-imports

    PATH = t.Union[os.PathLike, str]
    FILE_CONTAINER = t.Union[Path, ds.Dataset, t.List[str]]

PARTITION_SIZE = 100000  # Number of rows to have in each partition
FILE_NAME_PADDING = 6


class ReaderIterator:
    """Class for helpful iterating over a flight stream reader and getting PyArrow Tables"""

    def __init__(self, reader: fl.FlightStreamReader) -> None:
        """Initialize a ReaderIterator

        Parameters
        ----------
        reader : fl.FlightStreamReader
            Reader to iterate over
        """
        self.reader = reader
        self.table_len = PARTITION_SIZE
        self.batches: t.List[pa.RecordBatch] = []
        self.row_count = 0
        self.__exhausted = False

    def __next__(self) -> pa.Table:
        """Get Next table

        Returns
        -------
        pa.Table
            The next table from the reader

        Raises
        ------
        StopIteration
            If reader is exhausted
        """
        if self.__exhausted:
            raise StopIteration()
        return self.next_table()

    def __iter__(self) -> t.Generator[pa.Table, None, None]:
        """Iterate over all the reader's tables

        Returns
        -------
        t.Generator[pa.Table, None, None]
            Yields PyArrow Tables

        Yields
        -------
        Iterator[t.Generator[pa.Table, None, None]]
            Yields Pyarrow Tables
        """
        while not self.__exhausted:
            yield self.next_table()

    def next_table(self, table_len: t.Optional[int] = None) -> t.Optional[pa.Table]:
        """Get the next table, optionally getting a different number of rows that the default

        Parameters
        ----------
        table_len : t.Optional[int], optional
            Table Length (number of rows) for the table, by default None

        Returns
        -------
        t.Optional[pa.Table]
            Table if there are more tables in the reader, else None
        """
        if self.__exhausted:
            return None
        table_len = self.table_len if table_len is None else table_len
        while True:
            try:
                c_data: pa.RecordBatch = self.reader.read_chunk().data
            except StopIteration:
                self.__exhausted = True
                if len(self.batches) > 0:
                    return pa.Table.from_batches(self.batches)
                return None
            else:
                if c_data.num_rows + self.row_count > table_len:
                    # If the number of rows and the row count should overflow into a new table,
                    # then get the last rows for the current table from the c_data and persist
                    # the remaining rows in that batch into `self.batches` for use when called next
                    break_point = table_len - self.row_count
                    self.batches.append(c_data.slice(0, break_point))
                    table = pa.Table.from_batches(self.batches)
                    self.batches = [c_data.slice(break_point)]
                    self.row_count = self.batches[0].num_rows
                    return table
                else:
                    # Otherwise, append this batch and add its length to `self.row_count`,
                    # then continue iterating over the reader
                    self.row_count += c_data.num_rows
                    self.batches.append(c_data)


def _format_file_index(index: int) -> str:
    """Format a file name with correct padding

    Parameters
    ----------
    index : int
        File index to format into file name

    Returns
    -------
    str
        File index cast to string and zfilled to desired width
    """
    return str(index).zfill(FILE_NAME_PADDING)


def _get_directory_path(file_container: FILE_CONTAINER) -> Path:
    """Get the directory, as a Path, for the provided File Container

    Parameters
    ----------
    file_container : FILE_CONTAINER
        Path, Dataset, of list of files paths

    Returns
    -------
    Path
        The parent directory for where files should be saved that are related to this file container
    """
    if isinstance(file_container, Path):
        if not file_container.exists():
            file_container.mkdir(parents=True)
        path = file_container
    else:
        file_path = (
            file_container[0]
            if isinstance(file_container, (tuple, list))
            else file_container.files[0]
        )
        parts = file_path.split("/")
        path = Path(
            "/", *parts[0 : len(parts) - 1]
        )  # Split the parts and remove the last one (the file name), adding a leading / to make it
        # an absolute path
    return path


def _get_last_file_path(file_container: FILE_CONTAINER) -> t.Optional[t.Tuple[str, int]]:
    """Get the File path of the file container's last file and its index

    Parameters
    ----------
    file_container : FILE_CONTAINER
        Path, Dataset, or List of file names

    Returns
    -------
    t.Optional[t.Tuple[str, int]]
        Tuple of [File Name, file's index], if there are files in the container, else None
    """
    dir_path = _get_directory_path(file_container)
    files = [f for f in dir_path.iterdir() if f.parts[-1].endswith(GLOBAL_CONFIG.PARQUET_EXTENSION)]
    if len(files) == 0:
        return None

    def get_index(file: Path) -> int:
        """Get the interger index of the file at this path

        Parameters
        ----------
        file : Path
            File Path to find index for, file should be of the form:
            /<some directories>/<some int castable string>.pqt

        Returns
        -------
        int
            Integer index of the file's name
        """
        return int(file.parts[-1].split(GLOBAL_CONFIG.PARQUET_EXTENSION)[0])

    max_file = files[0]
    max_index = get_index(max_file)
    for file in files[1:]:
        idx = get_index(file)
        if idx > max_index:
            max_index = idx
            max_file = file
    return str(max_file), max_index


def _get_next_file_path(file_container: FILE_CONTAINER) -> Path:
    """Get the file path for where the next file in the container should go

    Parameters
    ----------
    file_container : FILE_CONTAINER
        Path, Dataset, or List of files names

    Returns
    -------
    Path
        Path in which the next file should be saved, the file will not exists yet

    Raises
    ------
    ValueError
        If container is a Dataset or list of file names, but there are no files, we cannot determine
        the next file, if the container is empty, it must be a path
    """
    path_tuple = _get_last_file_path(file_container)
    if path_tuple is not None:
        path, index = path_tuple
        next_index = index + 1
        return Path(path.replace(_format_file_index(index), _format_file_index(next_index)))
    else:
        if isinstance(file_container, Path) and file_container.is_dir():
            return Path(file_container) / (_format_file_index(0) + GLOBAL_CONFIG.PARQUET_EXTENSION)

        raise ValueError(
            "No current files in the container, empty container path can only be determine if\
                the container is a `Path` instance"
        )


def _write_next_file(
    table: pa.Table, dir: FILE_CONTAINER, metadata_collector: t.List[pq.FileMetaData] = None
):
    """Look in the directory to determine what the next file name should be and write the table
    as a parquet file in that location

    Parameters
    ----------
    table : pa.Table
        PyArrow Table to write
    dir : FILE_CONTAINER
        FILE_CONTAINER to write the file in
    metadata_collector : t.List[pq.FileMetaData]
        Collector to pass to the write table to track meta data
    """
    next_file_path = _get_next_file_path(dir)
    pq.write_table(table, next_file_path, metadata_collector=metadata_collector)
    if metadata_collector is not None:
        fp = next_file_path.parts[-1]
        metadata_collector[-1].set_file_path(fp)


def _write_metadata_file(
    schema: pa.Schema, file_container: FILE_CONTAINER, metadata_collector: t.List[pq.FileMetaData]
):
    """Create a new MetaData file in the provided directory

    Parameters
    ----------
    schema : pa.Schema
        Schema for the MetaData file
    file_container : PATH
        Path to directory to put the file in
    metadata_collector : t.List[pq.FileMetaData]
        FileMetaData list to write
    """
    dir_path = _get_directory_path(file_container)

    pq.write_metadata(schema, dir_path / "_metadata", metadata_collector)


def _to_tables(dataset: ds.Dataset) -> t.Generator[t.Tuple[pa.Table, str], None, None]:
    """Create a Generator of tables and their file paths from a dataset

    Parameters
    ----------
    dataset : ds.Dataset
        Dataset to get tables for

    Returns
    -------
    t.Generator[pa.Table, None, None]
        Generator of tables, each table is one file of the dataset

    Yields
    -------
    Iterator[t.Generator[pa.Table, None, None]]
        Generator of tables, each table is one file of the dataset
    """
    for file in dataset.files:
        yield pq.read_table(file), file


def merge_reader(dataset: ds.Dataset, reader: fl.FlightStreamReader):
    """Merge the provided reader's results into the data set. The reader should be made up
    of new columns. For example:

    Dataset Schema:
    |col1:int|col2:float|col3:bool|

    Reader Schema:
    |newcol1:str|newcol2:float|newcol3:int|

    Resultant Schema:
    |col1:int|col2:float|col3:bool|newcol1:str|newcol2:float|newcol3:int|


    Parameters
    ----------
    dataset : ds.Dataset
        Dataset to merge into
    reader : fl.FlightStreamReader
        Reader to source results from
    """
    iterator = ReaderIterator(reader)
    current: pa.Table
    file: str
    additional: pa.Table
    schema = None
    metadata_collector: t.List[pq.FileMetaData] = []
    for (current, file), additional in zip(_to_tables(dataset), iterator):
        if current.num_rows != additional.num_rows:
            raise ValueError("Tables are Different Length, cannot merge")
        for col_name in additional.column_names:
            if col_name in current.column_names:
                continue
            current = current.append_column(col_name, additional.column(col_name))
        pq.write_table(current, file, metadata_collector=metadata_collector)
        metadata_collector[-1].set_file_path(Path(file).parts[-1])
        if schema is None:
            schema = current.schema
    _write_metadata_file(schema, dataset, metadata_collector)


def _get_and_update_metadata(
    dataset: ds.Dataset, skip_path: t.Optional[str] = None
) -> t.List[pq.FileMetaData]:
    """Get a list of File MetaData for the data set, updating their file path and
    optional skipping a path

    Parameters
    ----------
    dataset : ds.Dataset
        Dataset to get metadata for
    skip_path : t.Optional[str]
        path to skip adding to the list

    Returns
    -------
    t.List[pq.FileMetaData]
        List of FileMetaData will file path's set
    """
    metadata_collector: t.List[pq.FileMetaData] = []
    for p in dataset.files:
        if skip_path is not None and p.endswith(skip_path):
            continue
        md = pq.read_metadata(p)
        md.set_file_path(Path(p).parts[-1])
        metadata_collector.append(md)
    return metadata_collector


def append_reader(dataset: ds.Dataset, reader: fl.FlightStreamReader):
    """Append the results from the reader to the end data set

    Parameters
    ----------
    dataset : ds.Dataset
        Dataset to append to
    reader : fl.FlightStreamReader
        Reader to source data from
    """
    iterator = ReaderIterator(reader)
    last_file_path = _get_last_file_path(dataset)
    if last_file_path is None:
        raise ValueError("No file in this data set, use `write_new_dataset`")
    path, _ = last_file_path
    lf_meta_data = pq.read_metadata(path)
    schema = pq.read_schema(path)
    metadata_collector = _get_and_update_metadata(dataset, path)

    if lf_meta_data.num_rows <= PARTITION_SIZE:
        # If there is still space in the last file, add to it
        diff = PARTITION_SIZE - lf_meta_data.num_rows
        current = pq.read_table(path)
        additional = iterator.next_table(diff)
        table = th.concat_tables(current, additional) if additional is not None else current
        Path(path).unlink()
        _write_next_file(table, _get_directory_path(dataset), metadata_collector)
    else:
        # Otherwise just append its metadata
        metadata_collector.append(lf_meta_data)

    for table in iterator:
        _write_next_file(table, dataset, metadata_collector)
    _write_metadata_file(schema, dataset, metadata_collector)


def write_new_dataset(
    path: PATH,
    reader: fl.FlightStreamReader,
):
    """Write the contents fo the reader to a new data set

    Parameters
    ----------
    path : t.Union[os.PathLike, str]
        Path location to write the new dataset
    reader : fl.FlightStreamReader
        Reader to source data from
    """
    path = Path(path) if not isinstance(path, Path) else path
    metadata_collector: t.List[pq.FileMetaData] = []
    iterator = ReaderIterator(reader)

    # Store the first table so it's schema can be used to write meta data after writing all
    # other tables. The last table can't be used in the event that reader size and partition size
    # exactly match, leaving the last table to be empty
    try:
        first_table = next(iterator)
    except StopIteration:
        return  # No tables in the reader
    else:
        _write_next_file(first_table, path, metadata_collector)

    # Write the rest of the partitions
    for table in iterator:
        _write_next_file(table, path, metadata_collector)

    _write_metadata_file(first_table.schema, path, metadata_collector)
