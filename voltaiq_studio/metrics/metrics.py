from enum import Enum
import google.protobuf.any_pb2 as pb
import json
import pandas as pd
import pyarrow as pa
import pyarrow.flight as fl
from typing import Dict, List, Any, Union
# pylint: disable=no-name-in-module
from .FlightSql_pb2 import CommandGetTables, CommandStatementQuery
from ..flight.request import get_client


MetricsSchema = Dict[str, str]


class MetricsType(Enum):
    """The types of metrics that can be stored."""
    DEVICE = "device"
    TEST_RECORD = "test_record"
    CYCLE = "cycle"


def store_metrics(metrics: Union[pa.Table, pd.DataFrame], type: MetricsType, create: bool = False):
    """Save the provided metrics to permanent storage.

    You must include the primary key column(s) for the metrics you are storing (for example, for
    device metrics, you must have a column called `device_uuid`). If any metric in this dataframe
    was stored previously, it will be overwritten.

    Parameters
    ----------
    metrics
        A PyArrow table or Pandas dataframe containing the metrics to store
    type
        The type of metrics to store
    create
        Set to `True` when you are creating new metrics. This helps prevent accidentally creating
        duplicate metrics because of misspellings.
    """
    # Convert from Pandas to pyarrow if necessary. 
    data = pa.table(metrics) if isinstance(metrics, pd.DataFrame) else metrics

    # Check schema for null data. This works if the metrics wasn't an int -
    # just need to make sure it's not null type.
    types = data.schema.types
    if pa.null() in types:
        for i, t in enumerate(types):
            if t == pa.null():
                # cast int64 for null data to process
                data_schema = data.schema.set(i, pa.field(data.column_names[i], pa.int64()))
                data = data.cast(data_schema)

    client = get_client()

    params = {
        "action": "metrics",
        "type": type.value,
        "create": create,
    }
    descriptor = fl.FlightDescriptor.for_command(json.dumps(params))

    writer, _ = client.do_put(descriptor, data.schema)
    writer.write(data)
    writer.close()


def _list_tables(include_schema: bool) -> pa.Table:
    """Execute the GetTables command on the Flight SQL server"""
    client = get_client()

    tables_command = CommandGetTables()
    tables_command.include_schema = include_schema
    message = pb.Any()
    message.Pack(tables_command)
    descriptor = fl.FlightDescriptor.for_command(message.SerializeToString())

    flight_info = client.get_flight_info(descriptor)
    stream = client.do_get(flight_info.endpoints[0].ticket)
    return pa.Table.from_batches(stream.to_reader())


def list_metrics_tables() -> List[str]:
    """Get a list of tables that can be used in metrics queries.

    The returned tables can be referenced in queries passed to `query_metrics`.

    Returns
    -------
    List[str]
        A list of table names that can be included in metrics queries
    """
    tables = _list_tables(False)
    return tables.column("table_name").to_pylist()


def _decode_schema(schema_bytes: bytes) -> MetricsSchema:
    """Decode the provide schema to a dict from column name to column type.

    Parameters
    ----------
    schema_bytes
        The encoded schema in Arrow IPC format

    Returns
    -------
    The decoded schema as a dictionary from column names to column types
    """
    schema = pa.ipc.open_stream(schema_bytes).schema
    # To get the appropriate Pandas data types, create an empty table and convert it to Pandas,
    # then get the schema from that
    empty = schema.empty_table().to_pandas()
    return {name: str(dtype) for name, dtype in zip(empty.columns, empty.dtypes)}


def list_metrics_tables_schemas() -> Dict[str, MetricsSchema]:
    """Get a list of tables that can be used in metrics queries along with their schemas.

    The returned tables can be referenced in queries passed to `query_metrics`.

    Returns
    -------
    Dict[str, Dict[str, str]]
        A dictionary mapping table names to the table's schema. The schema is a dictionary mapping
        column names to strings representing the column types.
    """
    tables = _list_tables(True)
    return {
        tables["table_name"][i].as_py(): _decode_schema(tables["table_schema"][i].as_py())
        for i in range(tables.num_rows)
    }


def query_metrics_arrow(query: str) -> pa.Table:
    """Run a query on metrics data and return the results as a PyArrow table.

    This function has the same parameters and behaviors as :func:`query_metrics` except for the
    return type.

    Parameters
    ----------
    query
        The query to execute

    Returns
    -------
    pa.Table
        A PyArrow table containing the results of the query
    """
    client = get_client()

    statement_query = CommandStatementQuery()
    statement_query.query = query
    message = pb.Any()
    message.Pack(statement_query)
    descriptor = fl.FlightDescriptor.for_command(message.SerializeToString())

    flight_info = client.get_flight_info(descriptor)
    stream = client.do_get(flight_info.endpoints[0].ticket)
    return pa.Table.from_batches(stream.to_reader())


def query_metrics(query: str) -> pd.DataFrame:
    """Run a query on metrics data and return the results as a Pandas dataframe.

    This is a read only interface. To create or update metrics, use `store_metrics`.

    The query can reference other Voltaiq data as well. For example, to get both the device name and
    the value of `metric1` for all devices, you can run a query like this:
    ``select device_uuid, metric1, d.name as device_name from device_metrics join device_data as d on device_uuid = d.uuid``.
    Use `list_metrics_tables` and `list_metrics_tables_schemas` to get a list of available tables
    and their schemas.

    Use DuckDB's SQL syntax: https://duckdb.org/docs/sql/introduction.

    Parameters
    ----------
    query
        The query to execute

    Returns
    -------
    pd.DataFrame
        A Pandas dataframe containing the results of the query
    """
    return query_metrics_arrow(query).to_pandas()
