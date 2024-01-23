#!/usr/bin/env python3
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import readline # noqa

import numpy as np
import pandas as pd
import typing as t
import xarray as xr

from sqlglot import parse_one, exp
from xarray.core.groupby import DatasetGroupBy

command_info = {
    ".exit": "To exit from the current session.",
    ".set": "To set the dataset uri as a shortened key. e.g. .set era5 gs://{BUCKET}/dataset-uri",
    ".show": "To list down dataset shortened key. e.g. .show or .show [key]",
    "[query]": "Any valid sql like query."
}

table_dataset_map = {} # To store dataset shortened keys for a single session.

operate = {
    "and" : lambda a, b: a & b,
    "or" : lambda a, b: a | b,
    "eq" : lambda a, b: a == b,
    "gt" : lambda a, b: a > b,
    "lt" : lambda a, b: a < b,
    "gte" : lambda a, b: a >= b,
    "lte" : lambda a, b: a <= b,
}

aggregate_function_map = {
    'avg': lambda x, y: x.mean(dim=y),
    'min': lambda x, y: x.min(dim=y),
    'max': lambda x, y: x.max(dim=y),
    'sum': lambda x, y: x.sum(dim=y),
}

timestamp_formats = {
    'time_date':"%Y-%m-%d",
    'time_month':"%Y-%m",
    'time_year': "%Y"
}

def parse(a: t.Union[xr.DataArray, str], b: t.Union[xr.DataArray, str]) -> t.Tuple[t.Union[xr.DataArray, str],
                                                                                   t.Union[xr.DataArray, str]]:
    """
    Parse input values 'a' and 'b' into NumPy arrays with compatible types for evaluation.

    Parameters:
    - a (Union[xr.DataArray, str]): The first input value.
    - b (Union[xr.DataArray, str]): The second input value.

    Returns:
    - Tuple[xr.DataArray, Union[np.float64, np.float32, np.datetime64]]: Parsed NumPy arrays 'a' and 'b'.
    """

    if isinstance(a, str):
        a, b = b, a
    arr_type = a.dtype.name
    if arr_type == 'float64':
        b = np.float64(b)
    elif arr_type == 'float32':
        b = np.float32(b)
    elif arr_type == 'datetime64[ns]':
        b = np.datetime64(b)
    return a, b


def evaluate(a: t.Union[xr.DataArray, str], b: t.Union[xr.DataArray, str], operator: str) -> xr.DataArray:
    """
    Evaluate the expression 'a operator b' using NumPy arrays.

    Parameters:
    - a (Union[xr.DataArray, str]): The first input value.
    - b (Union[xr.DataArray, str]): The second input value.
    - operator (str): The operator to be applied.

    Returns:
    - xr.DataArray: The result of the evaluation.
    """
    a, b = parse(a, b)
    return operate[operator](a, b)


def inorder(expression: exp.Expression, ds: xr.Dataset) -> xr.DataArray:
    """
    Evaluate an expression using an xarray Dataset and return the result.

    Parameters:
    - expression (exp.Expression): The expression to be evaluated.
    - ds (xr.Dataset): The xarray Dataset used for evaluation.

    Returns:
    - xr.DataArray: The result of evaluating the expression on the given dataset.
    """

    if(expression.key == "identifier"):
        return ds[expression.args['this']]

    if(expression.key == "literal"):
        return expression.args['this']

    args = expression.args

    left = args['this']
    right = None
    if 'expression' in args:
        right = args['expression']

    left_sol = inorder(left, ds)

    right_sol = None
    if right is not None:
        right_sol = inorder(right, ds)

    if right_sol is not None:
        return evaluate(left_sol, right_sol, expression.key)
    else:
        return left_sol


def apply_order_by(fields: t.List[str], ds: xr.Dataset) -> xr.Dataset:
    """
    Apply order-by to the dataset based on specified fields.

    Parameters:
    - fields (List[str]): List of fields(coordinates) to be used for ordering.
    - ds (xarray.Dataset): The input dataset.

    Returns:
    - xarray.Dataset: The dataset after applying group-by and aggregation operations.
    """
    ordered_ds = ds
    for field in fields:
        actual_field = field.split()
        ordered_ds = (
            ordered_ds.sortby(actual_field[0], False) if
            len(actual_field) > 1 and actual_field[1] == 'DESC' else
            ordered_ds.sortby(actual_field[0])
        )
    return ordered_ds


def aggregate_variables(agg_funcs: t.List[t.Dict[str, str]],
                        ds: xr.Dataset,
                        time_fields: t.List[str],
                        coords_to_squeeze: t.List[str]) -> xr.Dataset:
    """
    Aggregate variables in an xarray dataset based on aggregation functions.

    Args:
        agg_funcs (List[Dict[str, str]]): List of dictionaries specifying aggregation functions for variables.
        ds (xr.Dataset): The input xarray dataset.
        time_fields (List[str]): List of time fields to consider for time-based grouping.
        coords_to_squeeze (List[str]): List of coordinates to be squeezed during aggregation.

    Returns:
        xr.Dataset: The aggregated xarray dataset.
    """
    agg_dataset = xr.Dataset(coords=ds.coords, attrs=ds.attrs)

    # Aggregate based on time fields
    if len(time_fields):
        agg_dataset = agg_dataset.groupby(ds['time'].dt.strftime(timestamp_formats[time_fields[0]]))
        agg_dataset = apply_aggregation(agg_dataset, 'avg', None)
        agg_dataset = agg_dataset.rename({"strftime": time_fields[0]})

    # Aggregate based on other coordinates
    agg_dataset = apply_aggregation(agg_dataset, 'avg', coords_to_squeeze)

    # Loop through aggregation functions
    for agg_func in agg_funcs:
        variable, function = agg_func['var'], agg_func['func']
        grouped_ds = ds[variable]
        dims = [value for value in coords_to_squeeze if value in ds[variable].coords]

        # If time fields are specified, group by time
        if len(time_fields):
            groups = grouped_ds.groupby(ds['time'].dt.strftime(timestamp_formats[time_fields[0]]))
            grouped_ds = apply_aggregation(groups, function, None)
            grouped_ds = grouped_ds.rename({"strftime": time_fields[0]})

        # Apply aggregation on dimensions
        agg_dim_ds = apply_aggregation(grouped_ds, function, dims)
        agg_dataset = agg_dataset.assign({f"{function}_{variable}": agg_dim_ds})

    return agg_dataset


def apply_group_by(time_fields: t.List[str], ds: xr.Dataset, agg_funcs: t.Dict[str, str],
                   coords_to_squeeze: t.List[str] = []) -> xr.Dataset:
    """
    Apply group-by and aggregation operations to the dataset based on specified fields and aggregation functions.

    Parameters:
    - time_fields (List[str]): List of time_fields(coordinates) to be used for grouping.
    - ds (xarray.Dataset): The input dataset.
    - agg_funcs (t.List[t.Dict[str, str]]): Dictionary mapping aggregation function names to their corresponding
    xarray-compatible string representations.
    - coords_to_squeeze (t.List[str]): The dimension along which to apply the aggregation.
        If None, aggregation is applied to the entire dataset.

    Returns:
    - xarray.Dataset: The dataset after applying group-by and aggregation operations.
    """

    grouped_ds = ds

    if len(time_fields) > 1:
        raise NotImplementedError("GroupBy using multiple time fields is not supported.")

    elif len(time_fields) == 1:
        grouped_ds = aggregate_variables(agg_funcs, ds, time_fields, coords_to_squeeze)

    return grouped_ds


def apply_aggregation(groups: t.Union[xr.Dataset, DatasetGroupBy], fun: str, dim: t.List[str] = []) -> xr.DataArray:
    """
    Apply aggregation to the groups based on the specified aggregation function.

    Parameters:
    - groups (Union[xr.Dataset, xr.core.groupby.DatasetGroupBy]): The input dataset or dataset groupby object.
    - fun (str): The aggregation function to be applied.
    - dim (Optional[str]): The dimension along which to apply the aggregation. If None, aggregation is applied
    to the entire dataset.

    Returns:
    - xr.Dataset: The dataset after applying the aggregation.
    """

    return aggregate_function_map[fun](groups, dim)


def get_coords_to_squeeze(fields: t.List[str], ds: xr.Dataset) -> t.List[str]:
    """
    Get the coordinates to squeeze from an xarray dataset.

    The function identifies coordinates in the dataset that are not part of the specified fields
    and are not the 'time' coordinate.

    Args:
        fields (List[str]): List of field names.
        ds (xr.Dataset): The xarray dataset.

    Returns:
        List[str]: List of coordinates to squeeze.
    """
    # Identify coordinates not in fields and not 'time'
    coords_to_squeeze = [coord for coord in ds.coords if coord not in fields and (coord != "time")]

    return coords_to_squeeze


def get_table(e: exp.Expression) -> str:
    """
    Get the table name from an expression.

    Args:
        e (Expression): The expression containing table information.

    Returns:
        str: The table name.
    """
    # Extract the table name from the expression
    table = e.find(exp.Table).args['this'].args['this']

    # Check if the table is mapped in table_dataset_map
    if table in table_dataset_map:
        table = table_dataset_map[table]

    return table


def parse_query(query: str) -> xr.Dataset:

    expr = parse_one(query)

    if not isinstance(expr, exp.Select):
        return "ERROR: Only select queries are supported."

    table = get_table(expr)

    is_star = expr.find(exp.Star)

    data_vars = []
    if is_star is None:
        data_vars = [var.args['this'].args['this'] if var.key == 'column' else var.args['this']
             for var in expr.expressions
             if (var.key == "column" or (var.key == "literal" and var.args.get("is_string") is True))]

    where = expr.find(exp.Where)
    group_by = expr.find(exp.Group)

    agg_funcs = [
        {
        'var': var.args['this'].args['this'].args['this'] if  var.args['this'].key == 'column'
                                                        else var.args['this'].args['this'],
        'func': var.key
        }
    for var in expr.expressions if var.key in aggregate_function_map
    ]

    if len(agg_funcs):
        data_vars = [ agg_var['var'] for agg_var in agg_funcs ]

    ds = xr.open_zarr(table)

    if is_star is None:
        ds = ds[data_vars]

    if where:
        mask = inorder(where, ds)
        ds = ds.where(mask, drop=True)

    coords_to_squeeze = None
    time_fields = []
    if group_by:
        fields = [ e.args['this'].args['this'] for e in group_by.args['expressions'] ]
        time_fields = list(filter(lambda field: "time" in field, fields))
        coords_to_squeeze = get_coords_to_squeeze(fields, ds)
        ds = apply_group_by(time_fields, ds, agg_funcs, coords_to_squeeze)

    if len(time_fields) == 0 and len(agg_funcs):
        coords_to_squeeze.append('time')
        aggregate_variables(agg_funcs, ds, time_fields, coords_to_squeeze)

    return ds


def set_dataset_table(cmd: str) -> None:
    """
    Set the mapping between a key and a dataset.

    Args:
        cmd (str): The command string in the format ".set key val"
            where key is the identifier and val is the dataset table.
    """
    # Split the command into parts
    cmd_parts = cmd.split(" ")

    # Check if the command has the correct number of arguments
    if len(cmd_parts) == 3:
        # Extract key and val from the command
        _, key, val = cmd_parts
        # Update the dataset table mapping
        table_dataset_map[key] = val
    else:
        # Print an error message for incorrect arguments
        print("Incorrect args. Run .help .set for usage info.")


def list_key_values(input: t.Dict[str, str]) -> None:
    """
    Display key-value pairs from a dictionary.

    Args:
        input (Dict[str, str]): The dictionary containing key-value pairs.
    """
    for cmd, desc in input.items():
        print(f"{cmd}  =>  {desc}")


def display_help(cmd: str) -> None:
    """
    Display help information for commands.

    Args:
        cmd (str): The command string.
    """
    cmd_parts = cmd.split(" ")

    if len(cmd_parts) == 2:
        if cmd_parts[1] in command_info:
            print(f"{cmd_parts[1]}  =>  {command_info[cmd_parts[1]]}")
        else:
            list_key_values(command_info)
    elif len(cmd_parts) == 1:
        list_key_values(command_info)
    else:
        print("Incorrect usage. Run .help or .help [cmd] for usage info.")


def display_table_dataset_map(cmd: str) -> None:
    """
    Display information from the table_dataset_map.

    Args:
        cmd (str): The command string.
    """
    cmd_parts = cmd.split(" ")

    if len(cmd_parts) == 2:
        if cmd_parts[1] in table_dataset_map:
            print(f"{cmd_parts[1]}  =>  {table_dataset_map[cmd_parts[1]]}")
        else:
            list_key_values(table_dataset_map)
    else:
        list_key_values(table_dataset_map)


def display_result(result: t.Any) -> None:
    """
    Display the result of a query.

    Args:
        result (Any): The result to be displayed.
    """
    if isinstance(result, xr.Dataset):
        if len(result.coords):
            print(result.to_dataframe().reset_index())
        else:
            result = result.compute().to_dict(data="list")
            df = pd.DataFrame({k: [v['data']] for k, v in result['data_vars'].items()})
            print(df)
    else:
        print(result)


def run_query(query: str) -> None:
    """
    Run a query and display the result.

    Args:
        query (str): The query to be executed.
    """
    result = parse_query(query)
    display_result(result)


def main():
    """
    Main function for runnning this file.
    """
    while True:

        query = input("xql> ")

        if query == ".exit":
            break

        elif ".help" in query:
            display_help(query)

        elif ".set" in query:
            set_dataset_table(query)

        elif ".show" in query:
            display_table_dataset_map(query)

        else:
            try:
                result = parse_query(query)
            except Exception as e:
                result = f"ERROR: {type(e).__name__}: {e.__str__()}."

            display_result(result)
