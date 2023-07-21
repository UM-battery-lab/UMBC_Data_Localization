"""Utility functions that are called by MATLAB"""

import pyarrow.parquet as pq
import tempfile
import typing as t

if t.TYPE_CHECKING:
    import pyarrow as pa


def call_table_function_for_matlab(func, args):
    """Allows the user to call any function that returns an Arrow table from MATLAB

    The results will be written to a Parquet file so that MATLAB can read them from disk. Only the
    file path is returned to the caller. This function should only be called from the
    `get_table_from_python_function` function in MATLAB.

    Parameters
    ----------
    func
        The Python function to run. The function must return a PyArrow table
    args
        The args to pass to the function as a tuple.

    Returns
    -------
    The path to a temporary Parquet file containing the results. The caller must delete this file
    when they are done with it.
    """
    try:
        table = func(*args)
    except TypeError:
        # There was only one argument so it doesn't need to be unpacked
        table = func(args)
    with tempfile.NamedTemporaryFile(prefix="vas_mat_", suffix=".pqt", delete=False) as outfile:
        pq.write_table(table, outfile)
        return outfile.name