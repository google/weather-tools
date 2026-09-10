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
import itertools
import json
import logging
import math
import os
import pandas as pd
import tempfile
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

from .sinks import ToDataSink, open_dataset, copy, open_local
from .util import (
    to_json_serializable_type,
    validate_region,
    _only_target_vars,
    get_coordinates,
    BQ_EXCLUDE_COORDS,
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
LATITUDE_COORD_CANDIDATES: t.Tuple[str, ...] = ('latitude', 'lat', 'y')
LONGITUDE_COORD_CANDIDATES: t.Tuple[str, ...] = ('longitude', 'lon', 'x')


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
        geo_data_parquet_path: A path to dump the geo data parquet. This parquet consists of columns:
          latitude, longitude, geo_point, and geo_polygon. We calculate all of this information
          upfront so that we do not need to process it every time we process a set of files.
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
        skip_creating_geo_data_parquet: Skip the generation of the geo data parquet if it already
          exists at the given --geo_data_parquet_path. Please note that the geo data parquet is mandatory
          for ingesting data into BigQuery.
        disable_grib_schema_normalization: Turn off grib's schema normalization; Default: normalization enabled.
        rows_chunk_size: The size of the chunk of rows to be loaded into memory for processing.
          Depending on your system's memory, use this to tune how much rows to process.

    .. _these docs: https://beam.apache.org/documentation/io/built-in/google-bigquery/#setting-the-insertion-method
    """
    output_table: str
    geo_data_parquet_path: str
    variables: t.List[str]
    area: t.List[float]
    import_time: t.Optional[datetime.datetime]
    infer_schema: bool
    xarray_open_dataset_kwargs: t.Dict
    tif_metadata_for_start_time: t.Optional[str]
    tif_metadata_for_end_time: t.Optional[str]
    skip_region_validation: bool
    disable_grib_schema_normalization: bool
    rows_chunk_size: int = 1_000_000
    skip_creating_polygon: bool = False
    skip_creating_geo_data_parquet: bool = False
    lat_grid_resolution: t.Optional[float] = None
    lon_grid_resolution: t.Optional[float] = None
    lat_coord_name: str = dataclasses.field(init=False, default='latitude')
    lon_coord_name: str = dataclasses.field(init=False, default='longitude')
    _coord_rename_map: t.Dict[str, str] = dataclasses.field(init=False, repr=False)

    @classmethod
    def add_parser_arguments(cls, subparser: argparse.ArgumentParser):
        subparser.add_argument('-o', '--output_table', type=str, required=True,
                               help="Full name of destination BigQuery table (<project>.<dataset>.<table>). Table "
                                    "will be created if it doesn't exist.")
        subparser.add_argument('--geo_data_parquet_path', type=str, required=True,
                               help="A path to dump the geo data parquet.")
        subparser.add_argument('--skip_creating_geo_data_parquet', action='store_true', default=False,
                               help="Skip the generation of geo data parquet if it already exists at given "
                                    "--geo_data_parquet_path. Please note that the geo data parquet is manditory for "
                                    " ingesting data into BigQuery. Default: off.")
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
        subparser.add_argument('-s', '--skip_region_validation', action='store_true', default=False,
                               help='Skip validation of regions for data migration. Default: off')
        subparser.add_argument('--rows_chunk_size', type=int, default=1_000_000,
                               help="The size of the chunk of rows to be loaded into memory for processing. "
                                    "Depending on your system's memory, use this to tune how much rows to process.")
        subparser.add_argument('--disable_grib_schema_normalization', action='store_true', default=False,
                               help="To disable grib's schema normalization. Default: off")

    @classmethod
    def validate_arguments(cls, known_args: argparse.Namespace, pipeline_args: t.List[str]) -> None:
        pipeline_options = PipelineOptions(pipeline_args)
        pipeline_options_dict = pipeline_options.get_all_options()

        if known_args.area:
            assert len(known_args.area) == 4, 'Must specify exactly 4 lat/long values for area: N, W, S, E boundaries.'

        # Add a check for group_common_hypercubes.
        if pipeline_options_dict.get('group_common_hypercubes'):
            raise RuntimeError('--group_common_hypercubes can be specified only for earth engine ingestions.')

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

        if not known_args.geo_data_parquet_path.endswith(".parquet"):
            raise RuntimeError(f"'--geo_data_parquet_path' {known_args.geo_data_parquet_path} must "
                                "end with '.parquet'.")

        # Check that Cloud resource regions are consistent.
        if not (known_args.dry_run or known_args.skip_region_validation):
            # Program execution will terminate on failure of region validation.
            logger.info('Validating regions for data migration. This might take a few seconds...')
            validate_region(known_args.output_table, temp_location=pipeline_options_dict.get('temp_location'),
                            region=pipeline_options_dict.get('region'))
            logger.info('Region validation completed successfully.')

    def generate_parquet(
        self,
        parquet_path: str,
        lats: t.List,
        lons: t.List,
        lat_grid_resolution: float,
        lon_grid_resolution: float,
        skip_creating_polygon: bool = False,
        lat_column_name: str = 'latitude',
        lon_column_name: str = 'longitude',
    ):
        """Generates geo data parquet."""
        logger.info("Generating geo data parquet ...")
        # Generate Cartesian product of latitudes and longitudes.
        lat_lon_pairs = itertools.product(lats, lons)
        # Create a temp parquet file for writing.
        with tempfile.NamedTemporaryFile(suffix='.parquet', mode='w+', newline='') as temp:
            # Define header.
            header = [lat_column_name, lon_column_name, GEO_POINT_COLUMN, GEO_POLYGON_COLUMN]
            data = []
            for lat, lon in lat_lon_pairs:
                lat = float(lat)
                lon = float(lon)
                row = [lat, lon]
                sanitized_lon = (((lon % 360) + 540) % 360) - 180

                # Fetch the geo point.
                geo_point = fetch_geo_point(lat, sanitized_lon)
                row.append(geo_point)

                # Fetch the geo polygon if not skipped.
                if not skip_creating_polygon:
                    geo_polygon = fetch_geo_polygon(lat, sanitized_lon, lat_grid_resolution, lon_grid_resolution)
                    row.append(geo_polygon)
                else:
                    row.append(None)
                data.append(row)

            df = pd.DataFrame(data, columns=header)
            # Write DataFrame to parquet.
            df.to_parquet(temp.name, index=False)
            logger.info(f"geo data parquet generated successfully. Uploading to {parquet_path}.")
            copy(temp.name, parquet_path)
            logger.info(f"geo data parquet uploaded successfully at {parquet_path}.")

    def __post_init__(self):
        """Initializes Sink by creating a BigQuery table based on user input."""
        if self.zarr:
            self.xarray_open_dataset_kwargs = self.zarr_kwargs
        with open_dataset(self.first_uri, self.xarray_open_dataset_kwargs,
                          self.disable_grib_schema_normalization, self.tif_metadata_for_start_time,
                          self.tif_metadata_for_end_time, is_zarr=self.zarr) as open_ds:
            source_lat, source_lon = self._resolve_spatial_coordinate_names(open_ds)
            self._coord_rename_map = self._build_coord_rename_map(source_lat, source_lon)
            open_ds = self._normalize_dataset_coords(open_ds)

            if not self.skip_creating_polygon:
                logger.warning(
                    "Assumes that equal distance between consecutive points of %s and %s for the entire grid.",
                    self.lat_coord_name,
                    self.lon_coord_name,
                )
                # Find the grid_resolution.
                if open_ds[self.lat_coord_name].size > 1 and open_ds[self.lon_coord_name].size > 1:
                    latitude_length = len(open_ds[self.lat_coord_name])
                    longitude_length = len(open_ds[self.lon_coord_name])

                    latitude_range = np.ptp(open_ds[self.lat_coord_name].values)
                    longitude_range = np.ptp(open_ds[self.lon_coord_name].values)

                    self.lat_grid_resolution = abs(latitude_range / latitude_length) / 2
                    self.lon_grid_resolution = abs(longitude_range / longitude_length) / 2

                else:
                    self.skip_creating_polygon = True
                    logger.warning("Polygon can't be genereated as provided dataset has a only single grid point.")
            else:
                logger.info("Polygon is not created as '--skip_creating_polygon' flag passed.")

            if not self.skip_creating_geo_data_parquet:
                if self.area:
                    n, w, s, e = self.area
                    open_ds = open_ds.sel(
                        {
                            self.lat_coord_name: slice(n, s),
                            self.lon_coord_name: slice(w, e),
                        }
                    )

                lats = open_ds[self.lat_coord_name].values.tolist()
                lons = open_ds[self.lon_coord_name].values.tolist()
                self.generate_parquet(
                    self.geo_data_parquet_path,
                    [lats] if isinstance(lats, float) else lats,
                    [lons] if isinstance(lons, float) else lons,
                    self.lat_grid_resolution,
                    self.lon_grid_resolution,
                    self.skip_creating_polygon,
                    lat_column_name=self.lat_coord_name,
                    lon_column_name=self.lon_coord_name,
                )
            else:
                logger.info("geo data parquet is not created as '--skip_creating_geo_data_parquet' flag passed.")

            # Define table from user input
            if self.variables and not self.infer_schema and not open_ds.attrs['is_normalized']:
                logger.info('Creating schema from input variables.')
                table_schema = to_table_schema(
                    [(self.lat_coord_name, 'FLOAT64'), (self.lon_coord_name, 'FLOAT64'), ('time', 'TIMESTAMP')] +
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

    def prepare_coordinates(self, uri: str) -> t.Iterator[t.Tuple[str, t.Dict]]:
        """Open the dataset, filter by area, and prepare chunks of coordinates for parallel ingestion into BigQuery."""
        logger.info(f'Preparing coordinates for: {uri!r}.')

        with open_dataset(uri, self.xarray_open_dataset_kwargs, self.disable_grib_schema_normalization,
                          self.tif_metadata_for_start_time, self.tif_metadata_for_end_time, is_zarr=self.zarr) as ds:
            ds = self._normalize_dataset_coords(ds)
            data_ds: xr.Dataset = _only_target_vars(ds, self.variables)
            for coordinate in get_coordinates(data_ds, uri):
                yield uri, coordinate

    def extract_rows(self, uri: str, coordinate: t.Dict) -> t.Iterator[t.Dict]:
        """Reads an asset and coordinates, then yields its rows as a mapping of column names to values."""
        logger.info(f'Extracting rows for {coordinate!r} of {uri!r}.')

        # Re-calculate import time for streaming extractions.
        if not self.import_time:
            self.import_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

        with open_dataset(uri, self.xarray_open_dataset_kwargs, self.disable_grib_schema_normalization,
                          self.tif_metadata_for_start_time, self.tif_metadata_for_end_time, is_zarr=self.zarr) as ds:
            ds = self._normalize_dataset_coords(ds)
            data_ds: xr.Dataset = _only_target_vars(ds, self.variables)
            if self.area:
                n, w, s, e = self.area
                data_ds = data_ds.sel(
                    {
                        self.lat_coord_name: slice(n, s),
                        self.lon_coord_name: slice(w, e),
                    }
                )
                logger.info(f'Data filtered by area, size: {data_ds.nbytes}')
            yield from self.to_rows(coordinate, data_ds, uri)

    def to_rows(self, coordinate: t.Dict, ds: xr.Dataset, uri: str) -> t.Iterator[t.Dict]:
        first_ts_raw = (
            ds.time[0].values if isinstance(ds.time.values, np.ndarray)
            else ds.time.values
        )
        first_time_step = to_json_serializable_type(first_ts_raw)
        with open_local(self.geo_data_parquet_path) as master_lat_lon:
            selected_ds = ds.loc[coordinate]

            # Ensure that the latitude and longitude dimensions are in sync with the geo data parquet.
            coord_pair = {self.lat_coord_name, self.lon_coord_name}
            if coord_pair.issubset(set(selected_ds.sizes.keys())):
                selected_ds = selected_ds.transpose(self.lat_coord_name, self.lon_coord_name)

            vector_df = pd.read_parquet(master_lat_lon)
            vector_df = self._align_vector_coordinate_columns(vector_df)
            if self.skip_creating_polygon:
                vector_df[GEO_POLYGON_COLUMN] = None

            # Add indexed coordinates.
            for k, v in coordinate.items():
                vector_df[k] = to_json_serializable_type(v)

            # Add un-indexed coordinates.
            # Filter out excluded coordinates from coords.
            filtered_coords = (
                c for c in selected_ds.coords if c not in {self.lat_coord_name, self.lon_coord_name}
            )
            for c in filtered_coords:
                if c not in coordinate and (not self.variables or c in self.variables):
                    vector_df[c] = to_json_serializable_type(ensure_us_time_resolution(selected_ds[c].values))

            # We are not directly assigning values to dataframe because we need to consider 'None'.
            # Vectorized operations are generally more faster and efficient than iterating over rows.
            # Furthermore, pd.Series does not enforces length consistency so just added a safety check.
            for var in selected_ds.data_vars:
                values = to_json_serializable_type(ensure_us_time_resolution(selected_ds[var].values.ravel()))
                if len(values) != len(vector_df):
                    raise ValueError(
                        f"Length of values {len(values)} does not match number of rows in DataFrame {len(vector_df)}."
                    )
                vector_df[var] = pd.Series(values, dtype=object)

            vector_df[DATA_IMPORT_TIME_COLUMN] = self.import_time
            vector_df[DATA_URI_COLUMN] = uri
            vector_df[DATA_FIRST_STEP] = first_time_step
            num_chunks = math.ceil(len(vector_df) / self.rows_chunk_size)
            logger.info(f"{uri!r} -- {coordinate!r}'s vector_df divided into {num_chunks} chunk(s).")
            for i in range(num_chunks):
                chunk = vector_df[i * self.rows_chunk_size:(i + 1) * self.rows_chunk_size]
                rows = chunk.to_dict('records')
                logger.info(f"{uri!r} -- {coordinate!r}'s rows for {i} chunk converted to dict.")
                yield from rows

    def chunks_to_rows(self, _, ds: xr.Dataset) -> t.Iterator[t.Dict]:
        uri = ds.attrs.get(DATA_URI_COLUMN, '')
        # Re-calculate import time for streaming extractions.
        if not self.import_time or self.zarr:
            self.import_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

        ds = self._normalize_dataset_coords(ds)

        for coordinate in get_coordinates(ds, uri):
            yield from self.to_rows(coordinate, ds, uri)

    def _build_coord_rename_map(self, source_lat: str, source_lon: str) -> t.Dict[str, str]:
        rename_map: t.Dict[str, str] = {}
        if source_lat and source_lat != self.lat_coord_name:
            rename_map[source_lat] = self.lat_coord_name
        if source_lon and source_lon != self.lon_coord_name:
            rename_map[source_lon] = self.lon_coord_name
        return rename_map

    def _normalize_dataset_coords(self, ds: xr.Dataset) -> xr.Dataset:
        """Rename dataset coordinates to the internal latitude/longitude names."""
        if not getattr(self, "_coord_rename_map", None):
            return ds
        missing = [source for source in self._coord_rename_map if source not in ds.coords]
        if missing:
            raise ValueError(
                f"Dataset is missing expected coordinate(s) {missing} required for normalization."
            )
        return ds.rename(self._coord_rename_map)

    def _align_vector_coordinate_columns(self, vector_df: pd.DataFrame) -> pd.DataFrame:
        """Ensures the geo parquet uses the same coordinate column names as the dataset."""

        def _find_alias(candidates: t.Tuple[str, ...]) -> t.Optional[str]:
            for candidate in candidates:
                if candidate in vector_df.columns:
                    return candidate
            return None

        rename_map: t.Dict[str, str] = {}
        if self.lat_coord_name not in vector_df.columns:
            source = _find_alias(LATITUDE_COORD_CANDIDATES)
            if source:
                rename_map[source] = self.lat_coord_name
        if self.lon_coord_name not in vector_df.columns:
            source = _find_alias(LONGITUDE_COORD_CANDIDATES)
            if source:
                rename_map[source] = self.lon_coord_name

        if rename_map:
            vector_df = vector_df.rename(columns=rename_map)

        missing = [
            coord for coord in (self.lat_coord_name, self.lon_coord_name) if coord not in vector_df.columns
        ]
        if missing:
            raise ValueError(
                f"Geo data parquet {self.geo_data_parquet_path} is missing coordinate columns: {missing}"
            )

        return vector_df

    def _resolve_spatial_coordinate_names(self, ds: xr.Dataset) -> t.Tuple[str, str]:
        """Detects the dataset's latitude and longitude coordinate names."""

        def _find_candidate(candidates: t.Tuple[str, ...]) -> t.Optional[str]:
            for candidate in candidates:
                if candidate in ds.coords or candidate in ds.dims:
                    return candidate
            return None

        lat_name = _find_candidate(LATITUDE_COORD_CANDIDATES)
        lon_name = _find_candidate(LONGITUDE_COORD_CANDIDATES)
        if not lat_name or not lon_name:
            raise ValueError(
                f"Unable to identify spatial coordinate names. "
                f"Checked latitude aliases {LATITUDE_COORD_CANDIDATES} and longitude aliases {LONGITUDE_COORD_CANDIDATES}."
            )

        return lat_name, lon_name

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
            start_date = xarray_open_dataset_kwargs.pop('start_date', None)
            end_date = xarray_open_dataset_kwargs.pop('end_date', None)
            ds, chunks = xbeam.open_zarr(self.first_uri, **xarray_open_dataset_kwargs)

            if start_date is not None and end_date is not None:
                ds = ds.sel(time=slice(start_date, end_date))

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
