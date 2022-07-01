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
# import tempfile
import argparse
import json
import io
import os
import re
import dataclasses
import logging
import shutil
import datetime
import rasterio
import cfgrib
import tempfile
import numpy as np
import typing as t
import ee
from google import auth

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.gcsio import WRITE_CHUNK_SIZE, DEFAULT_READ_BUFFER_SIZE
from apache_beam.io import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions
from google.api_core.exceptions import BadRequest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

from .sinks import ToDataSink, open_datasets, open_local
from .util import to_json_serializable_type, _only_target_vars

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# A constant for all the things in the coords key set that aren't the
# level name.
_COORD_DISTRACTOR_SET = frozenset(('latitude', 'time', 'step', 'valid_time', 'longitude'))


def _to_python_timestamp(np_time: np.datetime64) -> float:
    """Turn a numpy datetime64 into floating point seconds."""
    return float((np_time - np.datetime64(0, 's')) / np.timedelta64(1, 's'))


def _get_bigstore_and_asset_path(
    channel_name: str,
    timestamp: float,
    forecast_hour: int,
    ee_asset: str,
    bucket_name: str,
) -> t.Tuple[str, str]:
    """Get the bigstore path and ee_asset_path for a given channel."""
    # Figure out the basename. HRRR only shows up hourly, so truncating the
    # timestamp to the nearst second should be totally safe.
    basename = f'{channel_name}_{int(timestamp)}_{forecast_hour}'
    bucket_path = os.path.join('/bigstore', bucket_name, basename + '.tiff')
    asset_path = os.path.join(ee_asset, channel_name, basename)
    return (bucket_path, asset_path)


@dataclasses.dataclass
class CogMetadata:
    """Metadata needed to register COGs in Earth Engine."""

    """Path to location of COG"""
    cog_path: str

    """Start time of when COG was created."""
    start_time: datetime.datetime

    """End time of when COG was created."""
    end_time: datetime.datetime

    """Names of the image channels to pass to Earth Engine."""
    channel_names: t.List[str]

    """An Earth Engine Metadata dict, taken from GDAL."""
    metadata: t.Dict[str, t.Union[int, float, str]]


@dataclasses.dataclass
class ChannelData(object):
    """A class for holding the output of the GetChannelsDoFn."""
    name: str  # The EE-safe name of the channel.
    bucket_path: str  # The bigstore path starting with /bigstore.
    asset_id: str  # The EE asset path of this particular file.
    start_time: float  # In floating point seconds since epoch.
    end_time: float  # In floating point seconds since epoch.
    projection: str  # The gdal string defining the projection.
    transform: t.Tuple[float, float, float, float, float, float]  # From gdal.
    metadata: t.Dict[str, t.Union[str, float, int]]
    _data: t.Optional[bytes] = None  # The encoded numpy data.

    @property
    def data(self) -> np.ndarray:
        """Return the parsed numpy data."""
        buffer = io.BytesIO(self._data)
        content = np.load(buffer)
        return content['data']

    @data.setter
    def data(self, new_data: np.ndarray):
        """Store the array data compressed to make flume happier."""
        buffer = io.BytesIO()
        np.savez_compressed(buffer, data=new_data)
        self._data = buffer.getvalue()


@dataclasses.dataclass
class ToEarthEngine(ToDataSink):
    """Loads weather data into Google Earth Engine.

    TODO(alxr): Document...
    """
    example_uri: str
    xarray_open_dataset_kwargs: t.Dict
    disable_in_memory_copy: bool
    tif_metadata_for_datetime: t.Optional[str]

    @classmethod
    def add_parser_arguments(cls, subparser: argparse.ArgumentParser):
        subparser.add_argument('--xarray_open_dataset_kwargs', type=json.loads, default='{}',
                               help='Keyword-args to pass into `xarray.open_dataset()` in the form of a JSON string.')
        subparser.add_argument('--disable_in_memory_copy', action='store_true', default=False,
                               help="To disable in-memory copying of dataset. Default: False")
        subparser.add_argument('--tif_metadata_for_datetime', type=str, default=None,
                               help='Metadata that contains tif file\'s timestamp. '
                                    'Applicable only for tif files.')

    @classmethod
    def validate_arguments(cls, known_args: argparse.Namespace, pipeline_args: t.List[str]) -> None:
        pipeline_options = PipelineOptions(pipeline_args)
        pipeline_options_dict = pipeline_options.get_all_options()
        # TODO: Add checks for arguments

    def __post_init__(self):
        """Initializes Sink by initialize earthengine based on user input."""

        if self.dry_run:
            logger.debug('Uploaded assets into earth engine ...')
            return

        # Authenticate & initialize earth engine
        try:
            # ee authenticate probably requires a system with browser support
            # ee.Authenticate()
            ee.Initialize()
        except Exception as e:
            logger.error(f'Unable to initialize earth engine: {e}')
            raise

    def expand(self, paths):
        """Convert gribs from paths into tiff and upload them into earth engine."""
        (
            paths
            |   'ConvertToCogs' >> beam.ParDo(
                    ConvertToCogs(
                        open_dataset_kwargs=self.xarray_open_dataset_kwargs,
                        disable_in_memory_copy=self.disable_in_memory_copy,
                        tif_metadata_for_datetime=self.tif_metadata_for_datetime)
                    )
            | 'ReshuffleFiles' >> beam.Reshuffle()
            | 'IngestIntoEE' >> beam.ParDo(
                IngestIntoEE()
            )
        )


@dataclasses.dataclass
class ConvertToCogs(beam.DoFn):
    """Converts weather data to COGs."""

    def __init__(self, 
                 open_dataset_kwargs: t.Optional[t.Dict] = None,
                 disable_in_memory_copy: bool = False,
                 tif_metadata_for_datetime: t.Optional[str] = None):
        self.open_dataset_kwargs = open_dataset_kwargs
        self.disable_in_memory_copy = disable_in_memory_copy
        self.tif_metadata_for_datetime = tif_metadata_for_datetime

    def _is_3d_da(self, da):
        """Checks whether data array is 3d or not."""
        return len(da.shape) == 3

    def process(self, uri: str) -> t.Iterator[ChannelData]:
        """Opens grib files and yields channel data."""

        logger.info(f'Converting {uri!r} to COGs ...')

        with open_datasets(uri,
                           self.open_dataset_kwargs,
                           self.disable_in_memory_copy,
                           self.tif_metadata_for_datetime) as ds_list:
            with open_local(uri) as local_path:
                with rasterio.open(local_path, 'r') as f:
                    logging.info(f'f.profile: {f.profile}')
                    dtype = f.profile.get('dtype')
                    crs = f.profile.get('crs')
                    transform = f.profile.get('transform')
            logging.info(f'dtype: {dtype}, crs: {crs}, transform: {transform}')

            # Protect this entire block with a try except to catch the various errors
            # that can happen during historical data ingestion.
            try:
                # Create per band tiff
                for ds in ds_list[:1]:
                    # Get the level by ignoring everything in coords that is "normal."
                    coords_set = set(ds.coords.keys())
                    level_set = coords_set.difference(_COORD_DISTRACTOR_SET)
                    level = level_set.pop()

                    # Now look at what data vars are in each level.
                    for key in ds.data_vars.keys():
                        da = ds[key]  # The data array.
                        attrs = da.attrs  # The metadata for this dataset.
                        logging.info("Reading attrs: %s" % attrs)

                        # Get the data into a numpy array.
                        data = np.array(da.data, dtype)

                        # We are going to treat the time field as start_time and the
                        # valid_time field as the end_time for EE purposes. Also, get the
                        # times into python-like floating point seconds timestamps.
                        start_time = _to_python_timestamp(da.time.values)
                        end_time = _to_python_timestamp(da.valid_time.values)

                        # Also figure out the forecast hour for this file.
                        forecast_hour = int(da.step.values / np.timedelta64(1, 'h'))

                        # Stick the forecast hour in the metadata, that's useful.
                        attrs['forecast_hour'] = forecast_hour
                        attrs['dtype'] = dtype

                        no_of_levels = da.shape[0] if self._is_3d_da(da) else 1

                        # Deal with the randomness that is 3d data interspersed with 2d.
                        # For 3d data, we need to extract ds for each level value.
                        for sub_c in range(no_of_levels):
                            height = da.coords[da.dims[0]].data[sub_c]

                            # Some heights are super small, but we can't have decimal points
                            # in channel names for Earth Engine, so mostly cut off the
                            # fractional part, unless we are forced to keep it. If so,
                            # replace the decimal point with yet another underscore.
                            if height >= 10:
                                height_string = f'{height:.0f}'
                            else:
                                height_string = f'{height:.2f}'.replace('.', '_')

                            channel_name = f'{level}_{attrs["GRIB_stepType"]}_{key}_{height_string}'
                            logging.info('Found channel %s', channel_name)

                            channel_data_array = data[sub_c, :, :] if self._is_3d_da(da) else data

                            # Add the height as a metadata field, that seems useful.
                            attrs['height'] = height

                            # # Get the bigstore path and asset path.
                            # bucket_path, ee_asset = _get_bigstore_and_asset_path(
                            #     channel_name=channel_name,
                            #     timestamp=start_time,
                            #     forecast_hour=forecast_hour,
                            #     ee_asset=self._ee_asset,
                            #     bucket_name=self._bucket,
                            # )
                            bucket_path, ee_asset = f'gs://rahul-tmp/deep/{channel_name}.tiff', f'projects/anthromet-prod/assets/{channel_name}'

                            # Now make the ChannelData object for this channel.
                            channel_data = ChannelData(
                                channel_name,
                                bucket_path=bucket_path,
                                asset_id=ee_asset,
                                start_time=start_time,
                                end_time=end_time,
                                projection=crs,
                                transform=transform,
                                metadata=attrs,
                            )

                            channel_data.data = channel_data_array

                            yield channel_data
            except Exception as e:  # pylint: disable=broad-except
                logging.exception('Error with %s', uri)
                raise e


def ee_basename(path: str) -> str:
    """Get an extension-free basename normalized for Earth Engine."""
    # Get the basename of the path (e.g. /foo/bar/baz.nc --> baz.nc)
    basename = os.path.basename(path)

    # Strip the extension off the basename.
    basename, _ = os.path.splitext(basename)

    # EE cannot have '.' in the basename. So, replace it with '_'...
    basename = basename.replace('.', '_')

    return basename


def destination_path(input_path: str, dest_bucket: str) -> str:
    """Turn a filepath into a destination bucket path."""
    return os.path.join(dest_bucket, ee_basename(input_path) + '.tiff')


@dataclasses.dataclass
class IngestIntoEE(beam.DoFn):
    """Writes tiff file, upload it to bucket and ingest into earth engine and yields task id"""

    def process(self, channel_data: ChannelData) -> t.Iterator[str]:
        """Writes tiff file, upload it to bucket and ingest into earth engine"""

        file_name = f'{channel_data.name}.tiff'
        logging.info(f'Writing {file_name} ...')

        with tempfile.TemporaryDirectory() as temp_dir:
            local_tiff_path = os.path.join(temp_dir, file_name)
            with rasterio.open(local_tiff_path, 'w',
                            driver='GTiff',
                            dtype=channel_data.metadata['dtype'],
                            width=channel_data.data.shape[1],
                            height=channel_data.data.shape[0],
                            count=1,
                            crs=channel_data.projection,
                            transform=channel_data.transform,
                            tiled=True) as f:
                f.write(channel_data.data, 1)

            logging.info('copying %s to %s', local_tiff_path, channel_data.bucket_path)
            with open(local_tiff_path, 'rb') as src:
                with FileSystems.create(channel_data.bucket_path) as dst:
                    shutil.copyfileobj(src, dst, WRITE_CHUNK_SIZE)

            manifest = {
                'name': channel_data.asset_id,
                'tilesets': [{
                    'crs': "epsg:4326",
                    'sources': [{
                        'uris': [channel_data.bucket_path]
                    }]
                }],
                'bands': [{
                    'id': channel_data.name,
                    'tileset_band_index': 0
                }],
                'start_time': {
                    'seconds': channel_data.start_time
                },
                'end_time': {
                    'seconds': channel_data.end_time
                },
                'properties': channel_data.metadata
            }
            logging.info(f'manifest: {manifest}')
            task_id = ee.data.newTaskId(1)[0]
            logging.info(f'task id: {task_id}')
            _ = ee.data.startIngestion(task_id, manifest)

            yield task_id
