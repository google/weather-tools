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
import io
import os
import re
import dataclasses
import logging
import shutil
import datetime
import rasterio
import cfgrib
import numpy as np
from typing import List, Dict, Optional, Tuple, Union
import ee
from google import auth

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.gcsio import WRITE_CHUNK_SIZE, DEFAULT_READ_BUFFER_SIZE

# from .sinks import ToDataSink
# from .util import deref

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# A constant for all the things in the coords key set that aren't the
# level name.
_COORD_DISTRACTOR_SET = frozenset(
    ('latitude', 'time', 'step', 'valid_time', 'longitude'))
# A constant for the terrible RE we need to fix the broken projection that gdal
# gives us.
_CENTRAL_MERIDIAN_RE = r'("central_meridian",)([0-9]+\.[0-9]+)'


def _to_python_timestamp(np_time: np.datetime64) -> float:
    """Turn a numpy datetime64 into floating point seconds."""
    return float((np_time - np.datetime64(0, 's')) / np.timedelta64(1, 's'))


def _get_bigstore_and_asset_path(
    channel_name: str,
    timestamp: float,
    forecast_hour: int,
    ee_asset: str,
    bucket_name: str,
) -> Tuple[str, str]:
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
    channel_names: List[str]

    """An Earth Engine Metadata dict, taken from GDAL."""
    metadata: Dict[str, Union[int, float, str]]


def _fix_broken_proj_def(projection: str) -> str:
    """If we had modern gdal or EE that could wrap, this wouldn't be here.

    The problem is that for reasons unknown, with the HRRR dataset, gdal decides
    to randomly add 360 to the central meridian definition sometimes. I don't know
    why this is, but using the grib_util just breaks everything, so that's not an
    option either. Even worse, this is only ever presented in string form, so I
    have to write regular expressions to fix this. The worst!

    Args:
    projection: The WKT string of a HRRR projection that is problably broken.

    Returns:
    A (hopefully) fixed WKT projection string that contains a reasonable value
    for the central_meridian field.
    """
    # First, find all of the central meridian instances so they can be checked.
    meridians = re.findall(_CENTRAL_MERIDIAN_RE, projection)

    # Loop over all the matching tuples and check them.
    for meridian_tuple in meridians:
        # Turn the second part of the tuple into a float for checking.
        meridian = float(meridian_tuple[1])

        if meridian > 180.0:
            # Subtract the 360 that gdal randomly and unexpectedly added.
            meridian -= 360.0

            logging.info('Found a bad meridian! Replacing %s with %0.1f',
                         meridian_tuple[1], meridian)

            # Substitute the new value back into the projection string.
            projection = projection.replace(
                ''.join(meridian_tuple),
                ''.join((meridian_tuple[0], f'{meridian:0.1f}'))
            )

    return projection


@dataclasses.dataclass
class ChannelData(object):
    """A class for holding the output of the GetChannelsDoFn."""
    name: str  # The EE-safe name of the channel.
    bucket_path: str  # The bigstore path starting with /bigstore.
    asset_id: str  # The EE asset path of this particular file.
    start_time: float  # In floating point seconds since epoch.
    end_time: float  # In floating point seconds since epoch.
    projection: str  # The gdal string defining the projection.
    transform: Tuple[float, float, float, float, float, float]  # From gdal.
    metadata: Dict[str, Union[str, float, int]]
    _data: Optional[bytes] = None  # The encoded numpy data.

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
class ToEarthEngine(beam.DoFn):
    """Loads weather data into Google Earth Engine.

    TODO(alxr): Document...
    """
    def __init__(self):
        # ee authenticate probably requires a system with browser support
        # ee.Authenticate()
        ee.Initialize()
        print('ee initialized')

    def process(self, channel_data: ChannelData):
        manifest = {
            'name': channel_data.asset_id,
            'tilesets': [{
                'id': channel_data.name,
                'crs': "epsg:4326",  # channel_data.projection
                'sources': [{
                    'uris': [channel_data.bucket_path]
                }]
            }],
            'bands': [{
                'id': channel_data.name,
                'tileset_id': channel_data.name,
                'tileset_band_index': 0
            }],
            'start_time': {
                'seconds': channel_data.start_time
            },
            'end_time': {
                'seconds': channel_data.end_time
            },
        }
        print(f'manifest: {manifest}')
        task_id = ee.data.newTaskId(1)[0]
        print(f'task id: {task_id}')
        print(ee.data.newTaskId)
        _ = ee.data.startIngestion(task_id, manifest)
        yield task_id


@dataclasses.dataclass
class ConvertToCogs(beam.DoFn):
    """Converts weather data to COGs and writes them to a cloud bucket."""

    def __init__(self, bucket: str):
        super().__init__()
        self.bucket = bucket

    def process(self, input_path, *args, **kwargs):

        logger.info(f'Converting {input_path!r} to COGs in {self.bucket!r}...')

        ds_list = cfgrib.open_datasets(input_path)
        # final_path = destination_path(input_path, self.bucket)

        # Create one big tiff
        with rasterio.open(input_path, 'r') as f:
            driver, dtype, nodata, width, height, count, crs, transform, tiled = list(f.profile.values())

        # with rasterio.open(final_path, 'w',
        #                    driver='GTiff',
        #                    dtype=dtype,
        #                    nodata=nodata,
        #                    width=width,
        #                    height=height,
        #                    count=count,
        #                    crs=crs,
        #                    transform=transform,
        #                    tiled=tiled) as f:
        #     band_index = 1
        #     for ds in ds_list:
        #         for _, v in ds.items():
        #             if len(v.data.shape) == 3:
        #                 for _v in v.data:
        #                     f.write(_v, band_index)
        #                     band_index += 1
        #                 continue

        #             f.write(v.data, band_index)
        #             band_index += 1

        # Protect this entire block with a try except to catch the various errors
        # that can happen during historical data ingestion.
        try:
            # Creat per band tiff

            for ds in ds_list[:1]:
                # Get the level by ignoring everything in coords that is "normal."
                # This is terrible.
                coords_set = set(ds.coords.keys())
                level_set = coords_set.difference(_COORD_DISTRACTOR_SET)
                level = level_set.pop()

                # Now look at what data vars are in each level.
                for key in ds.data_vars.keys():
                    da = ds[key]  # The data array.
                    attrs = da.attrs  # The metadata for this dataset.

                    # Get the data into a numpy array.
                    data = np.array(da.data, np.float32)

                    # We are going to treat the time field as start_time and the
                    # valid_time field as the end_time for EE purposes. Also, get the
                    # times into python-like floating point seconds timestamps.
                    start_time = _to_python_timestamp(da.time.values)
                    end_time = _to_python_timestamp(da.valid_time.values)

                    # Also figure out the forecast hour for this file.
                    forecast_hour = int(da.step.values /
                                        np.timedelta64(1, 'h'))

                    # Stick the forecast hour in the metadata, that's useful.
                    attrs['forecast_hour'] = forecast_hour
                    attrs['dtype'] = dtype

                    # Deal with the randomness that is 3d data interspersed with 2d.
                    if len(da.shape) == 3:
                        # We have 3d data, figure out what levels exist...
                        for sub_c in range(da.shape[0]):
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

                            channel_data_array = data[sub_c, :, :]

                            logging.info('Found channel %s', channel_name)

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
                    else:
                        # If we just have 2d data, we have a different format for the
                        # channel name...
                        height = da.coords[da.dims[0]].data[sub_c]
                        if height >= 10:
                            height_string = f'{height:.0f}'
                        else:
                            height_string = f'{height:.2f}'.replace('.', '_')

                        channel_name = f'{level}_{attrs["GRIB_stepType"]}_{key}_{height_string}'
                        logging.info('Found channel %s', channel_name)

                        # # Get the bigstore path and asset path.
                        # bucket_path, ee_asset = _get_bigstore_and_asset_path(
                        #     channel_name=channel_name,
                        #     timestamp=start_time,
                        #     forecast_hour=forecast_hour,
                        #     ee_asset=self._ee_asset,
                        #     bucket_name=self._bucket,
                        # )
                        bucket_path, ee_asset = f'gs://rahul-tmp/deep/{channel_name}.tiff', f'projects/anthromet-prod/assets/{channel_name}'

                        # Create the ChannelData object here too.
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

                        channel_data.data = data

                        yield channel_data
        except Exception as e:  # pylint: disable=broad-except
            if self._skip_exceptions:
                # If we're skipping the exception make sure it is logged.
                logging.exception('Skipping error with %s', input_path)
            else:
                # If we're not skipping the exception, make sure to log the path that
                # caused it as this can be very useful debugging information.
                logging.exception('Error with %s', input_path)
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


class WriteCogDoFn(beam.DoFn):
    """A DoFn to write the cog where it is supposed to go."""

    def __init__(self, overwrite_in_bucket: bool = False):
        self._overwrite_in_bucket = overwrite_in_bucket

    def process(self, channel_data: ChannelData):
        """Take the channel data and write it out as a cog."""
        file_path = f'{channel_data.name}.tiff'
        print(f'Writing {file_path} ...')
        logging.info('Writing temp file to %s', file_path)

        with rasterio.open(file_path, 'w',
                           driver='GTiff',
                           dtype=channel_data.metadata['dtype'],
                           width=channel_data.data.shape[1],
                           height=channel_data.data.shape[0],
                           count=1,
                           crs=channel_data.projection,
                           transform=channel_data.transform,
                           tiled=True) as f:
            f.write(channel_data.data, 1)

        logging.info('copying %s to %s', file_path, channel_data.bucket_path)
        with open(file_path, 'rb') as src:
            with FileSystems.create(channel_data.bucket_path) as dst:
                shutil.copyfileobj(src, dst, WRITE_CHUNK_SIZE)

        yield file_path

