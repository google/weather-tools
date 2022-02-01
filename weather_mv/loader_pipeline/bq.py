# Copyright 2022 Google LLC
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

import dataclasses
import datetime
import itertools
import logging
import operator
import typing as t
from functools import reduce

import apache_beam as beam
import numpy as np
import pandas as pd
import xarray as xr
from apache_beam.io import WriteToBigQuery, BigQueryDisposition
from google.cloud import bigquery
from xarray.core.utils import ensure_us_time_resolution

from .sinks import ToDataSink, open_dataset

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_IMPORT_TIME = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc).isoformat()
DATA_IMPORT_TIME_COLUMN = 'data_import_time'
DATA_URI_COLUMN = 'data_uri'
DATA_FIRST_STEP = 'data_first_step'


@dataclasses.dataclass
class ToBigQuery(ToDataSink):
    """Load weather data into Google BigQuery.

    TODO(alxr): Document
    """
    example_uri: str
    output_table: str
    infer_schema: bool
    import_time: datetime.datetime

    def __post_init__(self):
        if self.variables and not self.infer_schema:
            logger.info('Creating schema from input variables.')
            table_schema = to_table_schema(
                [('latitude', 'FLOAT64'), ('longitude', 'FLOAT64'), ('time', 'TIMESTAMP')] +
                [(var, 'FLOAT64') for var in self.variables]
            )
        else:
            logger.info('Inferring schema from data.')
            with open_dataset(next(iter(self.example_uri)), self.xarray_open_dataset_kwargs) as open_ds:
                ds: xr.Dataset = _only_target_vars(open_ds, self.variables)
                table_schema = dataset_to_table_schema(ds)

        # Create the table in BigQuery
        try:
            table = bigquery.Table(self.output_table, schema=table_schema)
            self.table = bigquery.Client().create_table(table, exists_ok=True)
        except Exception as e:
            logger.error(f'Unable to create table in BigQuery: {e}')
            raise

    def expand(self, paths):
        (
                paths
                | 'ExtractRows' >> beam.FlatMap(
                    extract_rows,
                    variables=self.variables,
                    area=self.area,
                    import_time=self.import_time,
                    open_dataset_kwargs=self.xarray_open_dataset_kwargs)
                | 'WriteToBigQuery' >> WriteToBigQuery(
                    project=self.table.project,
                    dataset=self.table.dataset_id,
                    table=self.table.table_id,
                    write_disposition=BigQueryDisposition.WRITE_APPEND,
                    create_disposition=BigQueryDisposition.CREATE_NEVER)

        )


def map_dtype_to_sql_type(var_type: np.dtype) -> str:
    """Maps a np.dtype to a suitable BigQuery column type."""
    if var_type in {np.dtype('float64'), np.dtype('float32'), np.dtype('timedelta64[ns]')}:
        return 'FLOAT64'
    elif var_type in {np.dtype('<M8[ns]')}:
        return 'TIMESTAMP'
    elif var_type in {np.dtype('int8'), np.dtype('int16'), np.dtype('int32'), np.dtype('int64')}:
        return 'INT64'
    raise ValueError(f"Unknown mapping from '{var_type}' to SQL type")


def dataset_to_table_schema(ds: xr.Dataset) -> t.List[bigquery.SchemaField]:
    """Returns a BigQuery table schema able to store the data in 'ds'."""
    # Get the columns and data types for all variables in the dataframe
    columns = [
        (str(col), map_dtype_to_sql_type(ds.variables[col].dtype))
        for col in ds.variables.keys() if ds.variables[col].size != 0
    ]

    return to_table_schema(columns)


def to_table_schema(columns: t.List[t.Tuple[str, str]]) -> t.List[bigquery.SchemaField]:
    # Fields are all Nullable because data may have NANs. We treat these as null.
    fields = [
        bigquery.SchemaField(column, var_type, mode='NULLABLE')
        for column, var_type in columns
    ]

    # Add an extra columns for recording import metadata.
    fields.append(bigquery.SchemaField(DATA_IMPORT_TIME_COLUMN, 'TIMESTAMP', mode='NULLABLE'))
    fields.append(bigquery.SchemaField(DATA_URI_COLUMN, 'STRING', mode='NULLABLE'))
    fields.append(bigquery.SchemaField(DATA_FIRST_STEP, 'TIMESTAMP', mode='NULLABLE'))

    return fields


def to_json_serializable_type(value: t.Any) -> t.Any:
    """Returns the value with a type serializable to JSON"""
    # Note: The order of processing is significant.
    logger.debug('Serializing to JSON')
    if np.issubdtype(type(value), np.floating):
        return float(value)
    elif type(value) == np.ndarray:
        # Will return a scaler if array is of size 1, else will return a list.
        return value.tolist()
    elif type(value) == datetime.datetime or type(value) == str or type(value) == np.datetime64:
        # Assume strings are ISO format timestamps...
        try:
            value = datetime.datetime.fromisoformat(value)
        except ValueError:
            # ... if they are not, assume serialization is already correct.
            return value
        except TypeError:
            # ... maybe value is a numpy datetime ...
            try:
                value = ensure_us_time_resolution(value).astype(datetime.datetime)
            except AttributeError:
                # ... value is a datetime object, continue.
                pass

        # We use a string timestamp representation.
        if value.tzname():
            return value.isoformat()

        # We assume here that naive timestamps are in UTC timezone.
        return value.replace(tzinfo=datetime.timezone.utc).isoformat()
    elif np.isnan(value) or value is None:
        return None
    elif type(value) == np.timedelta64:
        # Return time delta in seconds.
        return float(value / np.timedelta64(1, 's'))
    # This check must happen after processing np.timedelta64 and np.datetime64.
    elif np.issubdtype(type(value), np.integer):
        return int(value)

    return value


def _only_target_vars(ds: xr.Dataset, data_vars: t.Optional[t.List[str]] = None) -> xr.Dataset:
    """If the user specifies target fields in the dataset, create a schema only from those fields."""

    # If there are no restrictions on data vars, include the whole dataset.
    if not data_vars:
        logger.info(f'target data_vars empty; using whole dataset; size: {ds.nbytes}')
        return ds

    assert all([dv in ds.data_vars or dv in ds.coords for dv in data_vars]), 'Target variable must be in original ' \
                                                                             'dataset. '

    dropped_ds = ds.drop_vars([v for v in ds.data_vars if v not in data_vars])
    logger.info(f'target-only dataset size: {dropped_ds.nbytes}')

    return dropped_ds


def _prod(xs: t.Iterable[int]) -> int:
    return reduce(operator.mul, xs, 1)


def get_coordinates(ds: xr.Dataset, uri: str = '') -> t.Iterator[t.Dict]:
    """Generates normalized coordinate dictionaries that can be used to index Datasets with `.loc[]`."""
    # Creates flattened iterator of all coordinate positions in the Dataset.
    #
    # Coordinates have been pre-processed to remove NaNs and to format datetime objects
    # to ISO format strings.
    #
    # Example: (-108.0, 49.0, '2018-01-02T22:00:00+00:00')
    coords = itertools.product(
        *(
            (
                to_json_serializable_type(v)
                for v in ensure_us_time_resolution(ds[c].variable.values).tolist()
            )
            for c in ds.coords.indexes
        )
    )
    # Give dictionary keys to a coordinate index.
    #
    # Example:
    #   {'longitude': -108.0, 'latitude': 49.0, 'time': '2018-01-02T23:00:00+00:00'}
    idx = 0
    total_coords = _prod(ds.coords.dims.values())
    for idx, it in enumerate(coords):
        if idx % 1000 == 0:
            logger.info(f'Processed {idx // 1000}k / {(total_coords / 1000):.2f}k coordinates for {uri!r}...')
        yield dict(zip(ds.coords.indexes, it))

    logger.info(f'Finished processing all {(idx / 1000):.2f}k coordinates.')


def extract_rows(uri: str, *,
                 variables: t.Optional[t.List[str]] = None,
                 area: t.Optional[t.List[int]] = None,
                 import_time: t.Optional[str] = DEFAULT_IMPORT_TIME,
                 open_dataset_kwargs: t.Optional[t.Dict] = None) -> t.Iterator[t.Dict]:
    """Reads named netcdf then yields each of its rows as a dict mapping column names to values."""
    logger.info(f'Extracting rows as dicts: {uri!r}.')

    # re-calculate import time for streaming extractions.
    if not import_time:
        import_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

    with open_dataset(uri, open_dataset_kwargs) as ds:
        data_ds: xr.Dataset = _only_target_vars(ds, variables)

        if area:
            n, w, s, e = area
            data_ds = data_ds.sel(latitude=slice(n, s), longitude=slice(w, e))
            logger.info(f'Data filtered by area, size: {data_ds.nbytes}')

        first_ts_raw = data_ds.time[0].values if data_ds.time.size > 1 else data_ds.time.values
        first_time_step = to_json_serializable_type(first_ts_raw)

        def to_row(it: t.Dict) -> t.Dict:
            """Produce a single row, or a dictionary of all variables at a point."""

            # Use those index values to select a Dataset containing one row of data.
            row_ds = data_ds.loc[it]

            # Create a Name-Value map for data columns. Result looks like:
            # {'d': -2.0187, 'cc': 0.007812, 'z': 50049.8, 'rr': None}
            temp_row = row_ds.to_pandas().apply(to_json_serializable_type)
            # Pandas coerces floating type None values back to NaNs, need to do an explicit replace after.
            row = temp_row.astype(object).where(pd.notnull(temp_row), None).to_dict()

            # Add indexed coordinates.
            row.update(it)
            # Add un-indexed coordinates.
            for c in row_ds.coords:
                if c not in it and (not variables or c in variables):
                    row[c] = to_json_serializable_type(ensure_us_time_resolution(row_ds[c].values))

            # Add import metadata.
            row[DATA_IMPORT_TIME_COLUMN] = import_time
            row[DATA_URI_COLUMN] = uri
            row[DATA_FIRST_STEP] = first_time_step

            # 'row' ends up looking like:
            # {'latitude': 88.0, 'longitude': 2.0, 'time': '2015-01-01 06:00:00', 'd': -2.0187, 'cc': 0.007812,
            #  'z': 50049.8, 'data_import_time': '2020-12-05 00:12:02.424573 UTC', ...}
            beam.metrics.Metrics.counter('Success', 'ExtractRows').inc()
            return row

        yield from map(to_row, get_coordinates(data_ds, uri))
