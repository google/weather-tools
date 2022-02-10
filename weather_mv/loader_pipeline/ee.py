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
import tempfile
import typing as t
import os
import dataclasses
import logging
import shutil
import datetime

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.gcsio import WRITE_CHUNK_SIZE, DEFAULT_READ_BUFFER_SIZE
from osgeo import gdal

from .sinks import ToDataSink
from .util import deref

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclasses.dataclass
class ToEarthEngine(ToDataSink):
    """Loads weather data into Google Earth Engine.

    TODO(alxr): Document...
    """
    def __post_init__(self):
        pass

    def expand(self, input_or_inputs):
        pass


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
class ConvertToCogs(beam.DoFn):
    """Converts weather data to COGs and writes them to a cloud bucket."""

    bucket: str

    def process(self, input_path, *args, **kwargs):

        logger.info(f'Converting {input_path!r} to COGs in {self.bucket!r}...')

        cog_driver = gdal.GetDriverByName('GTiff')

        with tempfile.TemporaryDirectory() as tmp_dir:
            local_file = os.path.join(tmp_dir, os.path.basename(input_path))

            with FileSystems.open(input_path) as src:
                with open(local_file, 'wb') as dst:
                    shutil.copyfileobj(src, dst, DEFAULT_READ_BUFFER_SIZE)

            ds = gdal.OpenEx(local_file, gdal.OF_MULTIDIM_RASTER)
            rg = ds.GetRootGroup()

            band_names = set(rg.GetMDArrayNames()) - {d.GetName() for d in rg.GetDimensions()}

            local_cogs = []
            # Extraction based on examples from these sources:
            # - https://gdal.org/tutorials/multidimensional_api_tut.html#in-python
            # - https://geoexamples.com/other/2019/02/08/cog-tutorial.html
            for band_name in band_names:
                band = rg.OpenMDArray(band_name)
                band_data = band.ReadAsArray(buffer_datatype=gdal.ExtendedDataType.Create(gdal.GDT_Float64))

                band_local_path = os.path.join(tmp_dir, f'{ee_basename(input_path)}_{band_name}')

                data_set = cog_driver.Create(
                    band_local_path, *band_data.shape[::-1], gdal.GDT_Float64,
                    options=['TILED=YES', 'COMPRESS=LZW', 'INTERLEAVE=BAND', 'COPY_SRC_OVERVIEWS=YES']
                )

                for i in range(band_data[-1]):
                    data_set.GetRasterBand(i + 1).WriteArray(band_data[i])

                data_set.BuildOverviews("NEAREST", [2, 4, 8, 16, 32, 64])

                data_set.FlushCache()

                # Do not remove! Needed to avoid memory leaks at the C++ layer...
                data_set = None

                # Keep track of freshly minted COGs...
                local_cogs.append(band_local_path)

            # upload each COG to a cloud bucket...
            for cog in local_cogs:
                final_path = destination_path(cog, self.bucket)
                with open(cog, 'rb') as src:
                    with FileSystems.create(final_path) as dst:
                        shutil.copyfileobj(src, dst, WRITE_CHUNK_SIZE)

                # TODO(alxr): Follow up with team to see if this is the right way to go about this...
                # yield CogMetadata(final_path, band_names)


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





