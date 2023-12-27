
import numpy as np
import xarray as xr
import typing as t

from sqlglot import parse_one, exp

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
    'avg': lambda x, y: x.mean(dim=y) if y else x.mean(),
    'min': lambda x, y: x.min(dim=y) if y else x.mean(),
    'max': lambda x, y: x.max(dim=y) if y else x.mean(),
}

def parse(a, b):
    arr_type = a.dtype.name
    if arr_type == 'float64':
        b = np.float64(b)
    elif arr_type == 'float32':
        b = np.float32(b)
    elif arr_type == 'datetime64[ns]':
        b = np.datetime64(b)
    return a, b

def evaluate(a: xr.DataArray, b, operator):
    a, b = parse(a, b)
    return operate[operator](a, b)

def inorder(exp, ds):
    if(exp.key == "identifier"):
        return ds[exp.args['this']]

    if(exp.key == "literal"):
        return exp.args['this']

    args = exp.args

    left = args['this']
    right = None
    if 'expression' in args:
        right = args['expression']

    left_sol = inorder(left, ds)

    right_sol = None
    if right is not None:
        right_sol = inorder(right, ds)

    if right_sol is not None:
        return evaluate(left_sol, right_sol, exp.key)
    else:
        return left_sol

def apply_group_by(fields, ds: xr.Dataset, agg_funcs):
    grouped_ds = ds
    for field in fields:
        field_parts = field.split("_")
        groupby_field = field_parts[0]
        if len(field_parts) > 1:
            groupby_field = f"{groupby_field}.{field_parts[1]}"
            groups = grouped_ds.groupby(groupby_field)
            grouped_ds = apply_aggregation(groups, list(agg_funcs.values())[0])
        else:
            grouped_ds = apply_aggregation(grouped_ds, list(agg_funcs.values())[0], field)
    return grouped_ds

def apply_aggregation(groups, fun: str, dim: t.Optional[str] = None):
    return aggregate_function_map[fun](groups, dim)

def parse_query(query: str) -> xr.Dataset:

    expr = parse_one(query)

    if not isinstance(expr, exp.Select):
        return "ERROR: Only select queries are supported."

    table = expr.find(exp.Table).args['this'].args['this']

    is_star = expr.find(exp.Star)

    data_vars = []
    if is_star is None:
        data_vars = [ var.args['this'].args['this'] for var in expr.expressions if var.key == "column" ]

    where = expr.find(exp.Where)

    group_by = expr.find(exp.Group)

    agg_funcs = {
        var.args['this'].args['this'].args['this']: var.key
        for var in expr.expressions if var.key in aggregate_function_map
    }

    if len(agg_funcs):
        data_vars = agg_funcs.keys()

    ds = xr.open_zarr(table, chunks=None)

    if is_star is None:
        ds = ds[data_vars]

    mask = inorder(where, ds)

    filterd_ds = ds.where(mask, drop=True)

    if group_by:
        groupby_fields = [ e.args['this'].args['this'] for e in group_by.args['expressions'] ]
        filterd_ds = apply_group_by(groupby_fields, filterd_ds, agg_funcs)

    return filterd_ds

if __name__ == "__main__":

    while True:

        query = input("xql>")

        if query == "exit":
            break

        result = parse_query(query)

        if isinstance(result, xr.Dataset):
            print(result.to_dataframe())
        else:
            print(result)
