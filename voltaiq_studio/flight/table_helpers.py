"""Module for PyArrow Table Helpers"""
from __future__ import annotations
import typing as t

import pyarrow as pa


def table_to_schema(schema: pa.Schema, table: pa.Table) -> pa.Table:
    """Cast a table to match the provided schema. The table must have all the columns that
    the schema requires, but can have more, columns not in the schema will be dropped

    Parameters
    ----------
    schema : pa.Schema
        Schema to cast table to match
    table : pa.Table
        Table to source data from

    Returns
    -------
    pa.Table
        Table matching the data shape provided in the schema

    Raises
    ------
    ValueError
        If table is missing any columns from the schema
    """
    if table.schema == schema:
        return table
    arrays = [table.column(name) for name in schema.names]
    return pa.table(arrays, schema)


def concat_tables(root_table: pa.Table, *tables: t.List[pa.Table]) -> pa.Table:
    """Concatenate tables. Root Table's schema will be used. Any columns in the tables that
    are not in the root table will be dropped

    Parameters
    ----------
    root_table : pa.Table
        Table to be used as basis for schema
    tables : t.List[pa.Table]
        Tables to append to the root table

    Returns
    -------
    pa.Table
        Table made up of root table with tables data added to the end
    """
    organized_tables = [table_to_schema(root_table.schema, _table) for _table in tables]
    return pa.concat_tables([root_table, *organized_tables])
