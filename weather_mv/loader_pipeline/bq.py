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
import logging
import typing as t

import apache_beam as beam
import geojson
import numpy as np
import pandas as pd
import xarray as xr
from apache_beam.io import WriteToBigQuery, BigQueryDisposition
from google.cloud import bigquery
from xarray.core.utils import ensure_us_time_resolution

from .sinks import ToDataSink, open_dataset
from .util import to_json_serializable_type, _only_target_vars, get_coordinates
from pprint import pformat

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_IMPORT_TIME = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc).isoformat()
DATA_IMPORT_TIME_COLUMN = 'data_import_time'
DATA_URI_COLUMN = 'data_uri'
DATA_FIRST_STEP = 'data_first_step'
GEO_POINT_COLUMN = 'geo_point'
LATITUDE_RANGE = (-90, 90)


@dataclasses.dataclass
class ToBigQuery(ToDataSink):
    """Load weather data into Google BigQuery.

    A sink that loads de-normalized weather data into BigQuery. First, this sink will
    create a BigQuery table from user input (either from `variables` or by inferring the
    schema). Next, it will convert the weather data into rows and then write each row to
    the BigQuery table.

    During a batch job, this transform will use the BigQueryWriter's file processing
    step, which requires that a `temp_location` is passed into the main CLI. This
    transform will perform streaming writes to BigQuery during a streaming Beam job. See
    `these docs`_ for more.

    Attributes:
        example_uri: URI to a weather data file, used to infer the BigQuery schema.
        output_table: The destination for where data should be written in BigQuery
        infer_schema: If true, this sink will attempt to read in an example data file
          read all its variables, and generate a BigQuery schema.
        import_time: The time when data was imported. This is used as a simple way to
          version data — variables can be distinguished based on import time. If None,
          the system will recompute the current time upon row extraction for each file.

    .. _these docs: https://beam.apache.org/documentation/io/built-in/google-bigquery/#setting-the-insertion-method
    """
    example_uri: str
    output_table: str
    infer_schema: bool
    import_time: t.Optional[datetime.datetime]

    def __post_init__(self):
        """Initializes Sink by creating a BigQuery table based on user input."""
        # Define table from user input
        if self.variables and not self.infer_schema:
            logger.info('Creating schema from input variables.')
            table_schema = to_table_schema(
                [('latitude', 'FLOAT64'), ('longitude', 'FLOAT64'), ('time', 'TIMESTAMP')] +
                [(var, 'FLOAT64') for var in self.variables]
            )
        else:
            logger.info('Inferring schema from data.')
            with open_dataset(self.example_uri, self.xarray_open_dataset_kwargs) as open_ds:
                ds: xr.Dataset = _only_target_vars(open_ds, self.variables)
                table_schema = dataset_to_table_schema(ds)

        if self.dry_run:
            logger.debug('Created the BigQuery table with schema...')
            logger.debug(f'\n{pformat(table_schema)}')
            return

        # Create the table in BigQuery
        try:
            table = bigquery.Table(self.output_table, schema=table_schema)
            self.table = bigquery.Client().create_table(table, exists_ok=True)
        except Exception as e:
            logger.error(f'Unable to create table in BigQuery: {e}')
            raise

    def expand(self, paths):
        """Extract rows of variables from data paths into a BigQuery table."""
        extracted_rows = (
                paths
                | 'ExtractRows' >> beam.FlatMap(
                    extract_rows,
                    variables=self.variables,
                    area=self.area,
                    import_time=self.import_time,
                    open_dataset_kwargs=self.xarray_open_dataset_kwargs)
            )

        if not self.dry_run:
            (
                extracted_rows
                | 'WriteToBigQuery' >> WriteToBigQuery(
                        project=self.table.project,
                        dataset=self.table.dataset_id,
                        table=self.table.table_id,
                        write_disposition=BigQueryDisposition.WRITE_APPEND,
                        create_disposition=BigQueryDisposition.CREATE_NEVER)
            )
        else:
            (
                extracted_rows
                | 'Log Extracted Rows' >> beam.Map(logger.debug)
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
    fields.append(bigquery.SchemaField(GEO_POINT_COLUMN, 'GEOGRAPHY', mode='NULLABLE'))

    return fields


def fetch_geo_point(lat: float, long: float) -> str:
    """Calculates a geography point from an input latitude and longitude."""
    if lat > LATITUDE_RANGE[1] or lat < LATITUDE_RANGE[0]:
        raise ValueError(f"Invalid latitude value '{lat}'")
    long = ((long + 180) % 360) - 180
    point = geojson.dumps(geojson.Point((long, lat)))
    return point


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
            row[GEO_POINT_COLUMN] = fetch_geo_point(row['latitude'], row['longitude'])

            # 'row' ends up looking like:
            # {'latitude': 88.0, 'longitude': 2.0, 'time': '2015-01-01 06:00:00', 'd': -2.0187, 'cc': 0.007812,
            #  'z': 50049.8, 'data_import_time': '2020-12-05 00:12:02.424573 UTC', ...}
            beam.metrics.Metrics.counter('Success', 'ExtractRows').inc()
            return row

        yield from map(to_row, get_coordinates(data_ds, uri))
