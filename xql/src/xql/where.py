import numpy as np
import typing as t
import xarray as xr

from sqlglot import exp

from .constant import CITIES_BOUNDING_BOXES, COUNTRIES_BOUNDING_BOXES, SUPPORTED_CUSTOM_COORDS

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

def get_sop_terms(expression: exp.Expression):
    def cross_product(exp1: t.List[exp.Expression], exp2: t.List[exp.Expression]):
        pdt = []
        if(len(exp1) == 0):
            return exp2
        if(len(exp2) == 0):
            return exp1
        for item1 in exp1:
            for item2 in exp2:
                term = item1.and_(item2)
                pdt.append(term)
        return pdt
    if expression.key == "and":
        children = list(expression.flatten())
        product_terms = []
        for child in children:
            terms = get_sop_terms(child)
            product_terms = cross_product(product_terms, terms)
        return product_terms
    elif expression.key == "or":
        children = list(expression.flatten())
        sum_terms = []
        for child in children:
            if child.key=="and" or child.key=="or":
                sum_terms.extend(get_sop_terms(child))
            else:
                sum_terms.append(child)
        return sum_terms
    else:
        return [expression]

def check_conditional(expression: exp.Expression) -> bool:
    k = expression.key
    if k == 'lt' or k == 'gt' or k == 'gte' or k == 'lte' or k == 'eq':
        return True
    return False

def parse_condition(expression: exp.Expression, condition_dict: dict) -> bool:
    # TODO: This function right now assumnes that for a condition
    # LHS is always Column name and RHS is always value
    op = expression.key
    args = expression.args

    left = args.get('this', None)
    right = args.get('expression', None)

    identifier = left.args.get('this')
    coordinate = identifier.args.get('this')
    value = right.args.get('this')

    if coordinate not in condition_dict:
        condition_dict[coordinate] = {}

    condition_dict[coordinate][op] = value

def is_ascending_order(da: xr.DataArray) -> bool:
    """Simple check if a dataarray is in increasing or descreasing order."""
    da = da.drop_duplicates(...)
    if(da[0].values < da[1].values):
        return True
    return False

def select_coordinate(da: xr.DataArray, coordinate: str, operator: str, value: any) -> xr.DataArray:
    """Based on operator and sort order (if data is in ascending or descending order)
    It will apply the greater or lesser condition.
    Equal condition does not depend on that.
    TODO: Explain what is happening here!
    TODO: Improve code.
    """
    op = operator
    da, parsed_value = parse(da, value)
    if op == 'eq':
        return da.sel({coordinate: parsed_value})
    if(is_ascending_order(da)):
        if op == 'gt' or op == 'gte':
            return da.sel({coordinate: slice(parsed_value, None)})
        elif op == 'lt' or op == 'lte':
            return da.sel({coordinate: slice(None, parsed_value)})
        else:
            raise ValueError(f"Unkown operator in select_coordinate op: {op}.")
    else:
        if op == 'gt' or op == 'gte':
            return da.sel({coordinate: slice(None, parsed_value)})
        elif op == 'lt' or op == 'lte':
            return da.sel({coordinate: slice(parsed_value, None)})
        else:
            raise ValueError(f"Unkown operator in select_coordinate op: {op}.")

def get_coords_condition(coordinate: str, condition: t.Dict[str, str]):
    """Generate a bounding box from the defined lat long ranges for cities / countries."""
    value = condition['eq'].lower()
    bounding_box = { 'latitude': { }, 'longitude': { } }
    if value in COUNTRIES_BOUNDING_BOXES:
        lat_min, lat_max, lon_min, lon_max  = COUNTRIES_BOUNDING_BOXES[value]
    elif value in CITIES_BOUNDING_BOXES:
        lat_min, lat_max, lon_min, lon_max  = CITIES_BOUNDING_BOXES[value]
    else:
        raise NotImplementedError(f"Can not query for {coordinate}:{value}")
    bounding_box['latitude']['gte'] = lat_min
    bounding_box['latitude']['lte'] = lat_max
    bounding_box['longitude']['gte'] = lon_min + 360 if lon_min < 0 else lon_min
    bounding_box['longitude']['lte'] = lon_max + 360 if lon_max < 0 else lon_max

    return bounding_box


def filter_condition_dict(condition_dict: dict, ds: xr.Dataset):
    """Filter out custom fields and update them with actual dataset supported coord.
    { 'country': 'new york' } => { 'latitude': {...}, 'longitude': {...} }
    """
    result = {}
    for coordinate, conditions in condition_dict.items():
        if coordinate in ds.coords:
            result[coordinate] = conditions
        elif coordinate in SUPPORTED_CUSTOM_COORDS:
            simple_coords_dict = get_coords_condition(coordinate, conditions)
            result.update(simple_coords_dict)
        else:
            raise NotImplementedError(f"Dataset can not be queried over {coordinate} field")
    return result

def apply_select_condition(ds: xr.Dataset, condition_dict: dict) -> xr.Dataset:
    """A condition dict will be in form
    {   'coord1': {'lt': val1, 'gt': val2},
        'coord2: {'eq': someval},
    ...}
    This function applies above conditions on the dataset.
    """
    absolute_condition_dict = {}
    condition_dict = filter_condition_dict(condition_dict, ds)
    for coordinate, conditions in condition_dict.items():
        coordinate_array = ds[coordinate]

        # Iterate each operation and apply on a data array.
        for operator, value in conditions.items():
            coordinate_array = select_coordinate(coordinate_array, coordinate,  operator, value)

        coordinate_values = coordinate_array.values

        # If the condition mentions lt(less than) or gt(greater than),
        # then remove the last or first element based on below conditions.
        if 'gt' in conditions:
            if is_ascending_order(coordinate_array):
                coordinate_values = np.delete(coordinate_values, 0)
            else:
                coordinate_values = np.delete(coordinate_values, -1)
        if 'lt' in conditions:
            if is_ascending_order(coordinate_array):
                coordinate_values = np.delete(coordinate_values, -1)
            else:
                coordinate_values = np.delete(coordinate_values, 0)

        absolute_condition_dict[coordinate] =  coordinate_values

    # Finally perform the select operation.
    return ds.sel(absolute_condition_dict)

def postorder(expression: exp.Expression, condition_dict: dict):
    """Performs post order traversal on sqlglot expression and converts it into a dict.
    The dict in updated in place.
    """
    if expression is None:
        return
    if expression.key == "literal" or expression.key == "identifier" or expression.key == "column":
        return

    args = expression.args

    left = args.get('this', None)
    right = args.get('expression', None)

    postorder(left, condition_dict)
    postorder(right, condition_dict)

    if(check_conditional(expression)):
        parse_condition(expression, condition_dict)

def apply_where(ds: xr.Dataset, expression: exp.Expression) -> xr.Dataset:
    terms = get_sop_terms(expression)
    or_ds = []
    for term in terms:
        condition_dict = {}
        postorder(term, condition_dict)
        reduced_ds = apply_select_condition(ds, condition_dict)
        or_ds.append(reduced_ds)
    # TODO: Merge all ds in or_ds together to create final
    # selected dataset.
    # As we are not supporting OR op for now. there will be a
    # single AND term.
    return or_ds[0]
