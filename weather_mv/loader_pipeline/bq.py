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
import os
import typing as t
from pprint import pformat

import apache_beam as beam
import geojson
import numpy as np
import xarray as xr
import xarray_beam as xbeam
from apache_beam.io import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window
from google.cloud import bigquery
from xarray.core.utils import ensure_us_time_resolution

from .sinks import ToDataSink, open_dataset
from .util import (
    to_json_serializable_type,
    validate_region,
    _only_target_vars,
    get_coordinates,
    ichunked,
)

logger = logging.getLogger(__name__)

DEFAULT_IMPORT_TIME = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=datetime.timezone.utc).isoformat()
DATA_IMPORT_TIME_COLUMN = 'data_import_time'
DATA_URI_COLUMN = 'data_uri'
DATA_FIRST_STEP = 'data_first_step'
GEO_POINT_COLUMN = 'geo_point'
GEO_POLYGON_COLUMN = 'geo_polygon'
LATITUDE_RANGE = (-90, 90)
LONGITUDE_RANGE = (-180, 180)


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
        tif_metadata_for_start_time: If the input is a .tif file, parse the tif metadata at
          this location for a start time / initialization time.
        tif_metadata_for_end_time: If the input is a .tif file, parse the tif metadata at
          this location for a end/forecast time.
        skip_region_validation: Turn off validation that checks if all Cloud resources
          are in the same region.
        disable_grib_schema_normalization: Turn off grib's schema normalization; Default: normalization enabled.
        coordinate_chunk_size: How many coordinates (e.g. a cross-product of lat/lng/time
          xr.Dataset coordinate indexes) to group together into chunks. Used to tune
          how data is loaded into BigQuery in parallel.

    .. _these docs: https://beam.apache.org/documentation/io/built-in/google-bigquery/#setting-the-insertion-method
    """
    output_table: str
    variables: t.List[str]
    area: t.List[float]
    import_time: t.Optional[datetime.datetime]
    infer_schema: bool
    xarray_open_dataset_kwargs: t.Dict
    tif_metadata_for_start_time: t.Optional[str]
    tif_metadata_for_end_time: t.Optional[str]
    skip_region_validation: bool
    disable_grib_schema_normalization: bool
    coordinate_chunk_size: int = 10_000
    skip_creating_polygon: bool = False
    lat_grid_resolution: t.Optional[float] = None
    lon_grid_resolution: t.Optional[float] = None
    start_date: t.Optional[str] = None
    end_date: t.Optional[str] = None

    @classmethod
    def add_parser_arguments(cls, subparser: argparse.ArgumentParser):
        subparser.add_argument('-o', '--output_table', type=str, required=True,
                               help="Full name of destination BigQuery table (<project>.<dataset>.<table>). Table "
                                    "will be created if it doesn't exist.")
        subparser.add_argument('-v', '--variables', metavar='variables', type=str, nargs='+', default=list(),
                               help='Target variables (or coordinates) for the BigQuery schema. Default: will import '
                                    'all data variables as columns.')
        subparser.add_argument('-a', '--area', metavar='area', type=float, nargs='+', default=list(),
                               help='Target area in [N, W, S, E]. Default: Will include all available area.')
        subparser.add_argument('--skip_creating_polygon', action='store_true',
                               help='Not ingest grid points as polygons in BigQuery. Default: Ingest grid points as '
                                    'Polygon in BigQuery. Note: This feature relies on the assumption that the '
                                    'provided grid has an equal distance between consecutive points of latitude and '
                                    'longitude.')
        subparser.add_argument('--import_time', type=str, default=datetime.datetime.utcnow().isoformat(),
                               help=("When writing data to BigQuery, record that data import occurred at this "
                                     "time (format: YYYY-MM-DD HH:MM:SS.usec+offset). Default: now in UTC."))
        subparser.add_argument('--infer_schema', action='store_true', default=False,
                               help='Download one file in the URI pattern and infer a schema from that file. Default: '
                                    'off')
        subparser.add_argument('--xarray_open_dataset_kwargs', type=json.loads, default='{}',
                               help='Keyword-args to pass into `xarray.open_dataset()` in the form of a JSON string.')
        subparser.add_argument('--tif_metadata_for_start_time', type=str, default=None,
                               help='Metadata that contains tif file\'s start/initialization time. '
                                    'Applicable only for tif files.')
        subparser.add_argument('--tif_metadata_for_end_time', type=str, default=None,
                               help='Metadata that contains tif file\'s end/forecast time. '
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
        if (uri_extension in ['.tif', '.tiff'] and not known_args.tif_metadata_for_start_time):
            raise RuntimeError("'--tif_metadata_for_start_time' is required for tif files.")
        elif uri_extension not in ['.tif', '.tiff'] and (
            known_args.tif_metadata_for_start_time
            or known_args.tif_metadata_for_end_time
        ):
            raise RuntimeError("'--tif_metadata_for_start_time' and "
                               "'--tif_metadata_for_end_time' can be specified only for tif files.")

        # Check that Cloud resource regions are consistent.
        if not (known_args.dry_run or known_args.skip_region_validation):
            # Program execution will terminate on failure of region validation.
            logger.info('Validating regions for data migration. This might take a few seconds...')
            validate_region(known_args.output_table, temp_location=pipeline_options_dict.get('temp_location'),
                            region=pipeline_options_dict.get('region'))
            logger.info('Region validation completed successfully.')

    def __post_init__(self):
        """Initializes Sink by creating a BigQuery table based on user input."""
        if self.zarr:
            self.xarray_open_dataset_kwargs = self.zarr_kwargs
            self.start_date = self.zarr_kwargs.get('start_date')
            self.end_date = self.zarr_kwargs.get('end_date')
        with open_dataset(self.first_uri, self.xarray_open_dataset_kwargs,
                          self.disable_grib_schema_normalization, self.tif_metadata_for_start_time,
                          self.tif_metadata_for_end_time, is_zarr=self.zarr) as open_ds:

            if not self.skip_creating_polygon:
                logger.warning("Assumes that equal distance between consecutive points of latitude "
                               "and longitude for the entire grid.")
                # Find the grid_resolution.
                if open_ds['latitude'].size > 1 and open_ds['longitude'].size > 1:
                    latitude_length = len(open_ds['latitude'])
                    longitude_length = len(open_ds['longitude'])

                    latitude_range = np.ptp(open_ds["latitude"].values)
                    longitude_range = np.ptp(open_ds["longitude"].values)

                    self.lat_grid_resolution = abs(latitude_range / latitude_length) / 2
                    self.lon_grid_resolution = abs(longitude_range / longitude_length) / 2

                else:
                    self.skip_creating_polygon = True
                    logger.warning("Polygon can't be genereated as provided dataset has a only single grid point.")
            else:
                logger.info("Polygon is not created as '--skip_creating_polygon' flag passed.")

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

    def prepare_coordinates(self, uri: str) -> t.Iterator[t.Tuple[str, t.List[t.Dict]]]:
        """Open the dataset, filter by area, and prepare chunks of coordinates for parallel ingestion into BigQuery."""
        logger.info(f'Preparing coordinates for: {uri!r}.')

        with open_dataset(uri, self.xarray_open_dataset_kwargs, self.disable_grib_schema_normalization,
                          self.tif_metadata_for_start_time, self.tif_metadata_for_end_time, is_zarr=self.zarr) as ds:
            data_ds: xr.Dataset = _only_target_vars(ds, self.variables)
            if self.area:
                n, w, s, e = self.area
                data_ds = data_ds.sel(latitude=slice(n, s), longitude=slice(w, e))
                logger.info(f'Data filtered by area, size: {data_ds.nbytes}')

            for chunk in ichunked(get_coordinates(data_ds, uri), self.coordinate_chunk_size):
                yield uri, list(chunk)

    def extract_rows(self, uri: str, coordinates: t.List[t.Dict]) -> t.Iterator[t.Dict]:
        """Reads an asset and coordinates, then yields its rows as a mapping of column names to values."""
        logger.info(f'Extracting rows for [{coordinates[0]!r}...{coordinates[-1]!r}] of {uri!r}.')

        # Re-calculate import time for streaming extractions.
        if not self.import_time:
            self.import_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

        with open_dataset(uri, self.xarray_open_dataset_kwargs, self.disable_grib_schema_normalization,
                          self.tif_metadata_for_start_time, self.tif_metadata_for_end_time, is_zarr=self.zarr) as ds:
            data_ds: xr.Dataset = _only_target_vars(ds, self.variables)
            yield from self.to_rows(coordinates, data_ds, uri)

    def to_rows(self, coordinates: t.Iterable[t.Dict], ds: xr.Dataset, uri: str) -> t.Iterator[t.Dict]:
        first_ts_raw = (
            ds.time[0].values if isinstance(ds.time.values, np.ndarray)
            else ds.time.values
        )
        first_time_step = to_json_serializable_type(first_ts_raw)
        for it in coordinates:
            # Use those index values to select a Dataset containing one row of data.
            row_ds = ds.loc[it]

            # Create a Name-Value map for data columns. Result looks like:
            # {'d': -2.0187, 'cc': 0.007812, 'z': 50049.8, 'rr': None}
            row = {n: to_json_serializable_type(ensure_us_time_resolution(v.values))
                   for n, v in row_ds.data_vars.items()}

            # Serialize coordinates.
            it = {k: to_json_serializable_type(v) for k, v in it.items()}

            # Add indexed coordinates.
            row.update(it)
            # Add un-indexed coordinates.
            for c in row_ds.coords:
                if c not in it and (not self.variables or c in self.variables):
                    row[c] = to_json_serializable_type(ensure_us_time_resolution(row_ds[c].values))

            # Add import metadata.
            row[DATA_IMPORT_TIME_COLUMN] = self.import_time
            row[DATA_URI_COLUMN] = uri
            row[DATA_FIRST_STEP] = first_time_step

            longitude = ((row['longitude'] + 180) % 360) - 180
            row[GEO_POINT_COLUMN] = fetch_geo_point(row['latitude'], longitude)
            row[GEO_POLYGON_COLUMN] = (
                fetch_geo_polygon(row["latitude"], longitude, self.lat_grid_resolution, self.lon_grid_resolution)
                if not self.skip_creating_polygon
                else None
            )
            # 'row' ends up looking like:
            # {'latitude': 88.0, 'longitude': 2.0, 'time': '2015-01-01 06:00:00', 'd': -2.0187, 'cc': 0.007812,
            #  'z': 50049.8, 'data_import_time': '2020-12-05 00:12:02.424573 UTC', ...}
            beam.metrics.Metrics.counter('Success', 'ExtractRows').inc()
            yield row

    def chunks_to_rows(self, _, ds: xr.Dataset) -> t.Iterator[t.Dict]:
        uri = ds.attrs.get(DATA_URI_COLUMN, '')
        # Re-calculate import time for streaming extractions.
        if not self.import_time or self.zarr:
            self.import_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        yield from self.to_rows(get_coordinates(ds, uri), ds, uri)

    def expand(self, paths):
        """Extract rows of variables from data paths into a BigQuery table."""
        if not self.zarr:
            extracted_rows = (
                paths
                | 'PrepareCoordinates' >> beam.FlatMap(self.prepare_coordinates)
                | beam.Reshuffle()
                | 'ExtractRows' >> beam.FlatMapTuple(self.extract_rows)
            )
        else:
            xarray_open_dataset_kwargs = self.xarray_open_dataset_kwargs.copy()
            xarray_open_dataset_kwargs.pop('chunks')
            ds, chunks = xbeam.open_zarr(self.first_uri, **xarray_open_dataset_kwargs)

            if self.start_date is not None and self.end_date is not None:
                ds = ds.sel(time=slice(self.start_date, self.end_date))

            ds.attrs[DATA_URI_COLUMN] = self.first_uri
            extracted_rows = (
                paths
                | 'OpenChunks' >> xbeam.DatasetToChunks(ds, chunks)
                | 'ExtractRows' >> beam.FlatMapTuple(self.chunks_to_rows)
                | 'Window' >> beam.WindowInto(window.FixedWindows(60))
                | 'AddTimestamp' >> beam.Map(timestamp_row)
            )

        if self.dry_run:
            return extracted_rows | 'Log Rows' >> beam.Map(logger.info)
        return (
            extracted_rows
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
    fields.append(bigquery.SchemaField(GEO_POINT_COLUMN, 'GEOGRAPHY', mode='NULLABLE'))
    fields.append(bigquery.SchemaField(GEO_POLYGON_COLUMN, 'GEOGRAPHY', mode='NULLABLE'))

    return fields


def timestamp_row(it: t.Dict) -> window.TimestampedValue:
    """Associate an extracted row with the import_time timestamp."""
    timestamp = it[DATA_IMPORT_TIME_COLUMN].timestamp()
    return window.TimestampedValue(it, timestamp)


def fetch_geo_point(lat: float, long: float) -> str:
    """Calculates a geography point from an input latitude and longitude."""
    if lat > LATITUDE_RANGE[1] or lat < LATITUDE_RANGE[0]:
        raise ValueError(f"Invalid latitude value '{lat}'")
    if long > LONGITUDE_RANGE[1] or long < LONGITUDE_RANGE[0]:
        raise ValueError(f"Invalid longitude value '{long}'")
    point = geojson.dumps(geojson.Point((long, lat)))
    return point


def fetch_geo_polygon(latitude: float, longitude: float, lat_grid_resolution: float, lon_grid_resolution: float) -> str:
    """Create a Polygon based on latitude, longitude and resolution.

    Example ::
        * - . - *
        |       |
        .   •   .
        |       |
        * - . - *
    In order to create the polygon, we require the `*` point as indicated in the above example.
    To determine the position of the `*` point, we find the `.` point.
    The `get_lat_lon_range` function gives the `.` point and `bound_point` gives the `*` point.
    """
    lat_lon_bound = bound_point(latitude, longitude, lat_grid_resolution, lon_grid_resolution)
    polygon = geojson.dumps(geojson.Polygon([[
        (lat_lon_bound[0][0], lat_lon_bound[0][1]),  # lower_left
        (lat_lon_bound[1][0], lat_lon_bound[1][1]),  # upper_left
        (lat_lon_bound[2][0], lat_lon_bound[2][1]),  # upper_right
        (lat_lon_bound[3][0], lat_lon_bound[3][1]),  # lower_right
        (lat_lon_bound[0][0], lat_lon_bound[0][1]),  # lower_left
    ]]))
    return polygon


def bound_point(latitude: float, longitude: float, lat_grid_resolution: float, lon_grid_resolution: float) -> t.List:
    """Calculate the bound point based on latitude, longitude and grid resolution.

    Example ::
        * - . - *
        |       |
        .   •   .
        |       |
        * - . - *
    This function gives the `*` point in the above example.
    """
    lat_in_bound = latitude in [90.0, -90.0]
    lon_in_bound = longitude in [-180.0, 180.0]

    lat_range = get_lat_lon_range(latitude, "latitude", lat_in_bound,
                                  lat_grid_resolution, lon_grid_resolution)
    lon_range = get_lat_lon_range(longitude, "longitude", lon_in_bound,
                                  lat_grid_resolution, lon_grid_resolution)
    lower_left = [lon_range[1], lat_range[1]]
    upper_left = [lon_range[1], lat_range[0]]
    upper_right = [lon_range[0], lat_range[0]]
    lower_right = [lon_range[0], lat_range[1]]
    return [lower_left, upper_left, upper_right, lower_right]


def get_lat_lon_range(value: float, lat_lon: str, is_point_out_of_bound: bool,
                      lat_grid_resolution: float, lon_grid_resolution: float) -> t.List:
    """Calculate the latitude, longitude point range point latitude, longitude and grid resolution.

    Example ::
        * - . - *
        |       |
        .   •   .
        |       |
        * - . - *
    This function gives the `.` point in the above example.
    """
    if lat_lon == 'latitude':
        if is_point_out_of_bound:
            return [-90 + lat_grid_resolution, 90 - lat_grid_resolution]
        else:
            return [value + lat_grid_resolution, value - lat_grid_resolution]
    else:
        if is_point_out_of_bound:
            return [-180 + lon_grid_resolution, 180 - lon_grid_resolution]
        else:
            return [value + lon_grid_resolution, value - lon_grid_resolution]
