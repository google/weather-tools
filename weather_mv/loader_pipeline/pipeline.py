# Copyright 2021 Google LLC
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
"""Pipeline for reflecting lots of NetCDF objects into a BigQuery table."""

import argparse
import contextlib
import datetime
import itertools
import json
import logging
import operator
import shutil
import tempfile
import typing as t
from functools import reduce

import apache_beam as beam
import apache_beam.metrics
import numpy as np
import pandas as pd
import xarray as xr
from apache_beam.io import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from google.cloud import bigquery
from xarray.core.utils import ensure_us_time_resolution

from .streaming import GroupMessagesByFixedWindows, ParsePaths

DEFAULT_IMPORT_TIME = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc).isoformat()

DATA_IMPORT_TIME_COLUMN = 'data_import_time'
DATA_URI_COLUMN = 'data_uri'
DATA_FIRST_STEP = 'data_first_step'

logger = logging.getLogger(__name__)


def configure_logger(verbosity: int) -> None:
    """Configures logging from verbosity. Default verbosity will show errors."""
    level = (40 - verbosity * 10)
    logging.getLogger(__package__).setLevel(level)
    logger.setLevel(level)


def _prod(xs: t.Iterable[int]) -> int:
    return reduce(operator.mul, xs, 1)


def _make_grib_dataset_inmem(grib_ds: xr.Dataset) -> xr.Dataset:
    # Copies all the vars to in-memory to reduce disk seeks everytime a single row is processed.
    # This also removes the need to keep the backing temp source file around.
    data_ds = grib_ds.copy(deep=True)
    for v in grib_ds.variables:
        if v not in data_ds.coords:
            data_ds[v].variable.values = grib_ds[v].variable.values
    return data_ds


def __open_dataset_file(filename: str, open_dataset_kwargs: t.Optional[t.Dict] = None) -> xr.Dataset:

    if open_dataset_kwargs:
        return _make_grib_dataset_inmem(xr.open_dataset(filename, **open_dataset_kwargs))

    # If no open kwargs are available, make educated guesses about the dataset.
    try:
        return xr.open_dataset(filename)
    except ValueError as e:
        e_str = str(e)
        if not ("Consider explicitly selecting one of the installed engines" in e_str and "cfgrib" in e_str):
            raise
    # Trying with explicit engine for cfgrib.
    try:
        return _make_grib_dataset_inmem(
            xr.open_dataset(filename, engine='cfgrib', backend_kwargs={'indexpath': ''}))
    except ValueError as e:
        if "multiple values for key 'edition'" not in str(e):
            raise

    logger.warning("Assuming grib edition 1.")
    # Try with edition 1
    # Note: picking edition 1 for now as it seems to get the most data/variables for ECMWF realtime data.
    # TODO(#54): Make this a more generic function that can take custom args from tool users.
    return _make_grib_dataset_inmem(xr.open_dataset(filename, engine='cfgrib',
                                                    backend_kwargs={'filter_by_keys': {'edition': 1}, 'indexpath': ''}))


@contextlib.contextmanager
def open_dataset(uri: str, open_dataset_kwargs: t.Optional[t.Dict] = None) -> t.Iterator[xr.Dataset]:
    """Open the dataset at 'uri' and return a xarray.Dataset."""
    try:
        # Copy netcdf or grib object from cloud storage, like GCS, to local file
        # so xarray can open it with mmap instead of copying the entire thing
        # into memory.
        with FileSystems().open(uri) as source_file:
            with tempfile.NamedTemporaryFile() as dest_file:
                shutil.copyfileobj(source_file, dest_file)
                dest_file.flush()
                dest_file.seek(0)
                xr_dataset: xr.Dataset = __open_dataset_file(dest_file.name, open_dataset_kwargs)

                logger.info(f'opened dataset size: {xr_dataset.nbytes}')

                beam.metrics.Metrics.counter('Success', 'ReadNetcdfData').inc()
                yield xr_dataset
    except Exception as e:
        beam.metrics.Metrics.counter('Failure', 'ReadNetcdfData').inc()
        logger.error(f'Unable to open file {uri!r}: {e}')
        raise


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


def pattern_to_uris(match_pattern: str) -> t.Iterable[str]:
    for match in FileSystems().match([match_pattern]):
        yield from [x.path for x in match.metadata_list]


def run(argv: t.List[str], save_main_session: bool = True):
    """Main entrypoint & pipeline definition."""
    parser = argparse.ArgumentParser(
        prog='weather-mv',
        description='Weather Mover loads weather data from cloud storage into Google BigQuery.'
    )
    parser.add_argument('-i', '--uris', type=str, required=True,
                        help="URI prefix matching input netcdf objects, e.g. 'gs://ecmwf/era5/era5-2015-'.")
    parser.add_argument('-o', '--output_table', type=str, required=True,
                        help="Full name of destination BigQuery table (<project>.<dataset>.<table>). Table will be "
                             "created if it doesn't exist.")
    parser.add_argument('-v', '--variables', metavar='variables', type=str, nargs='+', default=list(),
                        help='Target variables (or coordinates) for the BigQuery schema. Default: will import all '
                             'data variables as columns.')
    parser.add_argument('-a', '--area', metavar='area', type=int, nargs='+', default=list(),
                        help='Target area in [N, W, S, E]. Default: Will include all available area.')
    parser.add_argument('--topic', type=str,
                        help="A Pub/Sub topic for GCS OBJECT_FINALIZE events, or equivalent, of a cloud bucket. "
                             "E.g. 'projects/<PROJECT_ID>/topics/<TOPIC_ID>'.")
    parser.add_argument("--window_size", type=float, default=1.0,
                        help="Output file's window size in minutes. Only used with the `topic` flag. Default: 1.0 "
                             "minute.")
    parser.add_argument('--num_shards', type=int, default=5,
                        help='Number of shards to use when writing windowed elements to cloud storage. Only used with '
                             'the `topic` flag. Default: 5 shards.')
    parser.add_argument('--import_time', type=str, default=datetime.datetime.utcnow().isoformat(),
                        help=("When writing data to BigQuery, record that data import occurred at this "
                              "time (format: YYYY-MM-DD HH:MM:SS.usec+offset). Default: now in UTC."))
    parser.add_argument('--infer_schema', action='store_true', default=False,
                        help='Download one file in the URI pattern and infer a schema from that file. Default: off')
    parser.add_argument('--xarray_open_dataset_kwargs', type=json.loads, default='{}',
                        help='Keyword-args to pass into `xarray.open_dataset()` in the form of a JSON string.')
    parser.add_argument('-d', '--dry-run', action='store_true', default=False,
                        help='Preview the load into BigQuery. Default: off')

    known_args, pipeline_args = parser.parse_known_args(argv[1:])

    if known_args.dry_run:
        raise NotImplementedError('dry-runs are currently not supported!')

    configure_logger(2)  # 0 = error, 1 = warn, 2 = info, 3 = debug

    if known_args.area:
        assert len(known_args.area) == 4, 'Must specify exactly 4 lat/long values for area: N, W, S, E boundaries.'

    # If a topic is used, then the pipeline must be a streaming pipeline.
    if known_args.topic:
        pipeline_args.extend('--streaming true'.split())

        # make sure we re-compute utcnow() every time rows are extracted from a file.
        known_args.import_time = None

    # Before starting the pipeline, read one file and generate the BigQuery
    # table schema from it. Assumes the number of matching uris is
    # manageable.
    all_uris = list(pattern_to_uris(known_args.uris))
    if not all_uris:
        raise FileNotFoundError(f"File prefix '{known_args.uris}' matched no objects")

    if known_args.variables and not known_args.infer_schema:
        logger.info('Creating schema from input variables.')
        table_schema = to_table_schema(
            [('latitude', 'FLOAT64'), ('longitude', 'FLOAT64'), ('time', 'TIMESTAMP')] +
            [(var, 'FLOAT64') for var in known_args.variables]
        )
    else:
        logger.info('Inferring schema from data.')
        with open_dataset(next(iter(all_uris)), known_args.xarray_open_dataset_kwargs) as open_ds:
            ds: xr.Dataset = _only_target_vars(open_ds, known_args.variables)
            table_schema = dataset_to_table_schema(ds)

    pipeline_options = PipelineOptions(pipeline_args)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # Create the table in BigQuery
    try:
        table = bigquery.Table(known_args.output_table, schema=table_schema)
        table = bigquery.Client().create_table(table, exists_ok=True)
    except Exception as e:
        logger.error(f'Unable to create table in BigQuery: {e}')
        raise

    with beam.Pipeline(options=pipeline_options) as p:
        if known_args.topic:
            paths = (
                    p
                    # Windowing is based on this code sample:
                    # https://cloud.google.com/pubsub/docs/pubsub-dataflow#code_sample
                    | 'ReadUploadEvent' >> beam.io.ReadFromPubSub(known_args.topic)
                    | 'WindowInto' >> GroupMessagesByFixedWindows(known_args.window_size, known_args.num_shards)
                    | 'ParsePaths' >> beam.ParDo(ParsePaths(known_args.uris))
            )
        else:
            paths = p | 'Create' >> beam.Create(all_uris)

        (
                paths
                | 'ExtractRows' >> beam.FlatMap(
                    extract_rows,
                    variables=known_args.variables,
                    area=known_args.area,
                    import_time=known_args.import_time,
                    open_dataset_kwargs=known_args.xarray_open_dataset_kwargs)
                | 'WriteToBigQuery' >> WriteToBigQuery(
                    project=table.project,
                    dataset=table.dataset_id,
                    table=table.table_id,
                    write_disposition=BigQueryDisposition.WRITE_APPEND,
                    create_disposition=BigQueryDisposition.CREATE_NEVER)
        )
    logger.info('Pipeline is finished.')
