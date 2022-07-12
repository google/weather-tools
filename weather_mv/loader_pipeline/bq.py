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
import argparse
import dataclasses
import datetime
import json
import logging
import math
import os
import signal
import sys
import tempfile
import traceback
import typing as t
import uuid
from functools import partial
from pprint import pformat
from urllib.parse import urlparse

import apache_beam as beam
import geojson
import numpy as np
import pandas as pd
import xarray as xr
from apache_beam.io import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions
from google.api_core.exceptions import BadRequest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

from .sinks import ToDataSink, open_dataset
from .util import (
    to_json_serializable_type,
    _only_target_vars,
    _only_target_coordinate_vars
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_IMPORT_TIME = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc).isoformat()
DATA_IMPORT_TIME_COLUMN = 'data_import_time'
DATA_URI_COLUMN = 'data_uri'
DATA_FIRST_STEP = 'data_first_step'
GEO_POINT_COLUMN = 'geo_point'
LATITUDE_RANGE = (-90, 90)

CANARY_BUCKET_NAME = 'anthromet_canary_bucket'
CANARY_RECORD = {'foo': 'bar'}
CANARY_RECORD_FILE_NAME = 'canary_record.json'
CANARY_OUTPUT_TABLE_SUFFIX = '_anthromet_canary_table'
CANARY_TABLE_SCHEMA = [bigquery.SchemaField('name', 'STRING', mode='NULLABLE')]


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
        variables: Target variables (or coordinates) for the BigQuery schema. By default,
          all data variables will be imported as columns.
        area: Target area in [N, W, S, E]; by default, all available area is included.
        import_time: The time when data was imported. This is used as a simple way to
          version data — variables can be distinguished based on import time. If None,
          the system will recompute the current time upon row extraction for each file.
        infer_schema: If true, this sink will attempt to read in an example data file
          read all its variables, and generate a BigQuery schema.
        xarray_open_dataset_kwargs: A dictionary of kwargs to pass to xr.open_dataset().
        disable_in_memory_copy: A flag to turn in-memory copy off; Default: in-memory copy enabled.
        tif_metadata_for_datetime: If the input is a .tif file, parse the tif metadata at
          this location for a timestamp.
        skip_region_validation: Turn off validation that checks if all Cloud resources
          are in the same region.
        disable_grib_schema_normalization: Turn off grib's schema normalization; Default: normalization enabled.
        coordinate_chunk_size: How many coordinates (e.g. a cross-product of lat/lng/time
          xr.Dataset coordinate indexes) to group together into chunks. Used to tune
          how data is loaded into BigQuery in parallel.

    .. _these docs: https://beam.apache.org/documentation/io/built-in/google-bigquery/#setting-the-insertion-method
    """
    example_uri: str
    output_table: str
    variables: t.List[str]
    area: t.Tuple[int, int, int, int]
    import_time: t.Optional[datetime.datetime]
    infer_schema: bool
    xarray_open_dataset_kwargs: t.Dict
    disable_in_memory_copy: bool
    tif_metadata_for_datetime: t.Optional[str]
    skip_region_validation: bool
    disable_grib_schema_normalization: bool
    coordinate_chunk_size: int = 10_000

    @classmethod
    def add_parser_arguments(cls, subparser: argparse.ArgumentParser):
        subparser.add_argument('-o', '--output_table', type=str, required=True,
                               help="Full name of destination BigQuery table (<project>.<dataset>.<table>). Table "
                                    "will be created if it doesn't exist.")
        subparser.add_argument('-v', '--variables', metavar='variables', type=str, nargs='+', default=list(),
                               help='Target variables (or coordinates) for the BigQuery schema. Default: will import '
                                    'all data variables as columns.')
        subparser.add_argument('-a', '--area', metavar='area', type=int, nargs='+', default=list(),
                               help='Target area in [N, W, S, E]. Default: Will include all available area.')
        subparser.add_argument('--import_time', type=str, default=datetime.datetime.utcnow().isoformat(),
                               help=("When writing data to BigQuery, record that data import occurred at this "
                                     "time (format: YYYY-MM-DD HH:MM:SS.usec+offset). Default: now in UTC."))
        subparser.add_argument('--infer_schema', action='store_true', default=False,
                               help='Download one file in the URI pattern and infer a schema from that file. Default: '
                                    'off')
        subparser.add_argument('--xarray_open_dataset_kwargs', type=json.loads, default='{}',
                               help='Keyword-args to pass into `xarray.open_dataset()` in the form of a JSON string.')
        subparser.add_argument('--disable_in_memory_copy', action='store_true', default=False,
                               help="To disable in-memory copying of dataset. Default: False")
        subparser.add_argument('--tif_metadata_for_datetime', type=str, default=None,
                               help='Metadata that contains tif file\'s timestamp. '
                                    'Applicable only for tif files.')
        subparser.add_argument('-s', '--skip-region-validation', action='store_true', default=False,
                               help='Skip validation of regions for data migration. Default: off')
        subparser.add_argument('--coordinate_chunk_size', type=int, default=10_000,
                               help='The size of the chunk of coordinates used for extracting vector data into '
                                    'BigQuery. Used to tune parallel uploads.')
        subparser.add_argument('--disable_grib_schema_normalization', action='store_true', default=False,
                               help="To disable grib's schema normalization. Default: off")

    @classmethod
    def validate_arguments(cls, known_args: argparse.Namespace, pipeline_args: t.List[str]) -> None:
        pipeline_options = PipelineOptions(pipeline_args)
        pipeline_options_dict = pipeline_options.get_all_options()

        if known_args.area:
            assert len(known_args.area) == 4, 'Must specify exactly 4 lat/long values for area: N, W, S, E boundaries.'

        # Check that all arguments are supplied for COG input.
        _, uri_extension = os.path.splitext(known_args.uris)
        if uri_extension == '.tif' and not known_args.tif_metadata_for_datetime:
            raise RuntimeError("'--tif_metadata_for_datetime' is required for tif files.")
        elif uri_extension != '.tif' and known_args.tif_metadata_for_datetime:
            raise RuntimeError("'--tif_metadata_for_datetime' can be specified only for tif files.")

        # Check that Cloud resource regions are consistent.
        if not (known_args.dry_run or known_args.skip_region_validation):
            # Program execution will terminate on failure of region validation.
            logger.info('Validating regions for data migration. This might take a few seconds...')
            validate_region(known_args.output_table, temp_location=pipeline_options_dict.get('temp_location'),
                            region=pipeline_options_dict.get('region'))
            logger.info('Region validation completed successfully.')

    def __post_init__(self):
        """Initializes Sink by creating a BigQuery table based on user input."""
        with open_dataset(self.example_uri, self.xarray_open_dataset_kwargs, True,
                          self.disable_grib_schema_normalization, self.tif_metadata_for_datetime) as open_ds:
            # Define table from user input
            if self.variables and not self.infer_schema and not open_ds.attrs['is_normalized']:
                logger.info('Creating schema from input variables.')
                table_schema = to_table_schema(
                    [('latitude', 'FLOAT64'), ('longitude', 'FLOAT64'), ('time', 'TIMESTAMP')] +
                    [(var, 'FLOAT64') for var in self.variables]
                )
            else:
                logger.info('Inferring schema from data.')
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
                | 'PrepareCoordinates' >> beam.FlatMap(
                    prepare_coordinates,
                    coordinate_chunk_size=self.coordinate_chunk_size,
                    area=self.area,
                    import_time=self.import_time,
                    open_dataset_kwargs=self.xarray_open_dataset_kwargs,
                    variables=self.variables,
                    disable_in_memory_copy=self.disable_in_memory_copy,
                    disable_grib_schema_normalization=self.disable_grib_schema_normalization,
                    tif_metadata_for_datetime=self.tif_metadata_for_datetime)
                | beam.Reshuffle()
                | 'ExtractRows' >> beam.FlatMapTuple(extract_rows)
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


def prepare_coordinates(
        uri: str, *,
        coordinate_chunk_size: int,
        variables: t.Optional[t.List[str]] = None,
        area: t.Optional[t.List[int]] = None,
        import_time: t.Optional[str] = DEFAULT_IMPORT_TIME,
        open_dataset_kwargs: t.Optional[t.Dict] = None,
        disable_in_memory_copy: bool = False,
        disable_grib_schema_normalization: bool = False,
        tif_metadata_for_datetime: t.Optional[str] = None) -> t.Iterator[t.Tuple[str, str, str, pd.DataFrame]]:
    """Open the dataset, filter by area, and prepare chunks of coordinates for parallel ingestion into BigQuery."""
    logger.info(f'Preparing coordinates for: {uri!r}.')

    with open_dataset(uri, open_dataset_kwargs, disable_in_memory_copy, disable_grib_schema_normalization,
                      tif_metadata_for_datetime) as ds:
        data_ds: xr.Dataset = _only_target_vars(ds, variables)
        if area:
            n, w, s, e = area
            data_ds = data_ds.sel(latitude=slice(n, s), longitude=slice(w, e))
            logger.info(f'Data filtered by area, size: {data_ds.nbytes}')

        # Re-calculate import time for streaming extractions.
        if not import_time:
            import_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

        first_ts_raw = data_ds.time[0].values if isinstance(data_ds.time.values, np.ndarray) else data_ds.time.values
        first_time_step = to_json_serializable_type(first_ts_raw)

        # Add coordinates for 0-dimension file.
        if len(data_ds.to_array().shape) == 1:
            for coord in data_ds.coords:
                data_ds = data_ds.assign_coords({coord: [data_ds[coord].values]})

        df = data_ds.to_dataframe().reset_index()

        # Add un-indexed coordinates and drop unwanted variables.
        if variables:
            indices = list(ds.coords.indexes.keys())
            to_keep = _only_target_coordinate_vars(ds, variables) + indices
            to_drop = set(df.columns) - set(to_keep)
            df.drop(to_drop, axis=1, inplace=True)

        num_chunks = math.ceil(len(df) / coordinate_chunk_size)
        for i in range(num_chunks):
            chunk = df[i * coordinate_chunk_size:(i + 1) * coordinate_chunk_size]
            yield uri, import_time, first_time_step, chunk


def _convert_time(val) -> t.Any:
    """Converts pandas Timestamp values to ISO format."""
    if isinstance(val, pd.Timestamp):
        return val.replace(tzinfo=datetime.timezone.utc).isoformat()
    elif isinstance(val, pd.Timedelta):
        return val.total_seconds()
    else:
        return val


def extract_rows(uri: str,
                 import_time: str,
                 first_time_step: str,
                 rows: pd.DataFrame) -> t.Iterator[t.Dict]:
    """Reads an asset and coordinates, then yields its rows as a mapping of column names to values."""
    for _, row in rows.iterrows():
        row = row.astype(object).where(pd.notnull(row), None)
        row = {k: _convert_time(v) for k, v in row.iteritems()}

        row[DATA_IMPORT_TIME_COLUMN] = import_time
        row[DATA_URI_COLUMN] = uri
        row[DATA_FIRST_STEP] = first_time_step
        row[GEO_POINT_COLUMN] = fetch_geo_point(row['latitude'], row['longitude'])

        beam.metrics.Metrics.counter('Success', 'ExtractRows').inc()
        yield row


def _cleanup(bigquery_client: bigquery.Client, storage_client: storage.Client, canary_output_table: str,
             canay_bucket_name: str, sig: t.Optional[t.Any] = None, frame: t.Optional[t.Any] = None) -> None:
    bigquery_client.delete_table(canary_output_table, not_found_ok=True)
    try:
        storage_client.get_bucket(canay_bucket_name).delete(force=True)
    except NotFound:
        pass
    if sig:
        traceback.print_stack(frame)
        sys.exit(0)


def validate_region(output_table: str, temp_location: t.Optional[str] = None, region: t.Optional[str] = None) -> None:
    """Validates non-compatible regions scenarios by performing sanity check."""
    if not region and not temp_location:
        raise ValueError('Invalid GCS location: None.')

    bigquery_client = bigquery.Client()
    storage_client = storage.Client()
    canary_output_table = output_table + CANARY_OUTPUT_TABLE_SUFFIX + str(uuid.uuid4())
    canary_bucket_name = CANARY_BUCKET_NAME + str(uuid.uuid4())

    # Doing cleanup if operation get cut off midway.
    # TODO : Should we handle some other signals ?
    do_cleanup = partial(_cleanup, bigquery_client, storage_client, canary_output_table, canary_bucket_name)
    original_sigint_handler = signal.getsignal(signal.SIGINT)
    original_sigtstp_handler = signal.getsignal(signal.SIGTSTP)
    signal.signal(signal.SIGINT, do_cleanup)
    signal.signal(signal.SIGTSTP, do_cleanup)

    bucket_region = region
    table_region = None

    if temp_location:
        parsed_temp_location = urlparse(temp_location)
        if parsed_temp_location.scheme != 'gs' or parsed_temp_location.netloc == '':
            raise ValueError(f'Invalid GCS location: {temp_location!r}.')
        bucket_name = parsed_temp_location.netloc
        bucket_region = storage_client.get_bucket(bucket_name).location

    try:
        bucket = storage_client.create_bucket(canary_bucket_name, location=bucket_region)
        with tempfile.NamedTemporaryFile(mode='w+') as temp:
            json.dump(CANARY_RECORD, temp)
            temp.flush()
            blob = bucket.blob(CANARY_RECORD_FILE_NAME)
            blob.upload_from_filename(temp.name)

        table = bigquery.Table(canary_output_table, schema=CANARY_TABLE_SCHEMA)
        table = bigquery_client.create_table(table, exists_ok=True)
        table_region = table.location

        load_job = bigquery_client.load_table_from_uri(
            f'gs://{canary_bucket_name}/{CANARY_RECORD_FILE_NAME}',
            canary_output_table,
        )
        load_job.result()
    except BadRequest:
        raise RuntimeError(f'Can\'t migrate from source: {bucket_region} to destination: {table_region}')
    finally:
        _cleanup(bigquery_client, storage_client, canary_output_table, canary_bucket_name)
        signal.signal(signal.SIGINT, original_sigint_handler)
        signal.signal(signal.SIGINT, original_sigtstp_handler)
