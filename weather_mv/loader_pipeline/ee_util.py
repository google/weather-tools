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
import os
import dataclasses
import logging
import shutil
import datetime
import rasterio
import tempfile
import typing as t
import ee
import xarray as xr

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.gcsio import DEFAULT_READ_BUFFER_SIZE
from apache_beam.options.pipeline_options import PipelineOptions

from .sinks import ToDataSink, open_dataset, open_local

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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
    grib_names: t.List[str]

    """An Earth Engine Metadata dict, taken from GDAL."""
    metadata: t.Dict[str, t.Union[int, float, str]]


@dataclasses.dataclass
class GribData(object):
    """A class for holding the output of the GetChannelsDoFn."""
    name: str  # The EE-safe name of the channel.
    bucket_path: str  # The bigstore path starting with /bigstore.
    asset_id: str  # The EE asset path of this particular file.
    start_time: float  # In floating point seconds since epoch.
    end_time: float  # In floating point seconds since epoch.
    dtype: str  # data type of the channel data
    projection: str  # The gdal string defining the projection.
    transform: t.Tuple[float, float, float, float, float, float]  # From gdal.
    metadata: t.Dict[str, t.Union[str, float, int]]
    data: t.List[xr.Dataset]  # The list of datasets


@dataclasses.dataclass
class ToEarthEngine(ToDataSink):
    """Loads weather data into Google Earth Engine.

    TODO(alxr): Document...
    """
    example_uri: str
    xarray_open_dataset_kwargs: t.Dict
    disable_in_memory_copy: bool
    disable_grib_schema_normalization: bool
    tif_metadata_for_datetime: t.Optional[str]

    @classmethod
    def add_parser_arguments(cls, subparser: argparse.ArgumentParser):
        subparser.add_argument('--xarray_open_dataset_kwargs', type=json.loads, default='{}',
                               help='Keyword-args to pass into `xarray.open_dataset()` in the form of a JSON string.')
        subparser.add_argument('--disable_in_memory_copy', action='store_true', default=False,
                               help="To disable in-memory copying of dataset. Default: False")
        subparser.add_argument('--disable_grib_schema_normalization', action='store_true', default=False,
                               help="To disable merge of grib datasets. Default: False")
        subparser.add_argument('--tif_metadata_for_datetime', type=str, default=None,
                               help='Metadata that contains tif file\'s timestamp. '
                                    'Applicable only for tif files.')

    @classmethod
    def validate_arguments(cls, known_args: argparse.Namespace, pipeline_args: t.List[str]) -> None:
        pipeline_options = PipelineOptions(pipeline_args)
        pipeline_options_dict = pipeline_options.get_all_options()
        logger.debug(f'Pipeline options: {pipeline_options_dict}')
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
            | 'ConvertToCogs' >> beam.ParDo(
                ConvertToCogs(
                    open_dataset_kwargs=self.xarray_open_dataset_kwargs,
                    disable_in_memory_copy=self.disable_in_memory_copy,
                    disable_grib_schema_normalization=self.disable_grib_schema_normalization,
                    tif_metadata_for_datetime=self.tif_metadata_for_datetime))
            | 'ReshuffleFiles' >> beam.Reshuffle()
            | 'IngestIntoEE' >> beam.ParDo(
                IngestIntoEE())
        )


@dataclasses.dataclass
class ConvertToCogs(beam.DoFn):
    """Converts weather data to COGs."""

    def __init__(self,
                 open_dataset_kwargs: t.Optional[t.Dict] = None,
                 disable_in_memory_copy: bool = False,
                 disable_grib_schema_normalization: bool = False,
                 tif_metadata_for_datetime: t.Optional[str] = None):
        self.open_dataset_kwargs = open_dataset_kwargs
        self.disable_in_memory_copy = disable_in_memory_copy
        self.disable_grib_schema_normalization = disable_grib_schema_normalization
        self.tif_metadata_for_datetime = tif_metadata_for_datetime

    def _create_grib_name(self, uri: str) -> str:
        file_name = os.path.basename(uri)
        base_name = os.path.splitext(file_name)[0]
        grib_name = base_name.replace('.', '_')
        return grib_name

    def process(self, uri: str) -> t.Iterator[GribData]:
        """Opens grib files and yields channel data."""

        logger.info(f'Converting {uri!r} to COGs ...')

        with open_dataset(uri,
                          self.open_dataset_kwargs,
                          self.disable_in_memory_copy,
                          self.disable_grib_schema_normalization,
                          self.tif_metadata_for_datetime) as ds:

            with open_local(uri) as local_path:
                with rasterio.open(local_path, 'r') as f:
                    dtype, crs, transform = (f.profile.get(key) for key in ['dtype', 'crs', 'transform'])

            # Creating one tiff
            attrs = ds.attrs
            grib_name = self._create_grib_name(uri)
            bucket_path = f'gs://rahul-tmp/deep/{grib_name}.tiff'
            ee_asset = f'projects/anthromet-prod/assets/{grib_name}'
            start_time, end_time = (attrs.get(key) for key in ('start_time', 'end_time'))

            grib_data = GribData(
                    grib_name,
                    bucket_path=bucket_path,
                    asset_id=ee_asset,
                    start_time=start_time,
                    end_time=end_time,
                    dtype=dtype,
                    projection=crs,
                    transform=transform,
                    metadata=attrs,
                    data=list(ds.values())
                )

            yield grib_data


@dataclasses.dataclass
class IngestIntoEE(beam.DoFn):
    """Writes tiff file, upload it to bucket and ingest into earth engine and yields task id"""

    def __init__(self):
        pass

    def process(self, grib_data: GribData) -> t.Iterator[str]:
        """Writes tiff file, upload it to bucket and ingest into earth engine"""

        file_name = f'{grib_data.name}.tiff'

        with tempfile.TemporaryDirectory() as temp_dir:
            local_tiff_path = os.path.join(temp_dir, file_name)
            with rasterio.open(local_tiff_path, 'w',
                               driver='GTiff',
                               dtype=grib_data.dtype,
                               width=grib_data.data[0].data.shape[1],
                               height=grib_data.data[0].data.shape[0],
                               count=len(grib_data.data),
                               crs=grib_data.projection,
                               transform=grib_data.transform,
                               tiled=True) as f:
                for i, da in enumerate(grib_data.data):
                    f.write(da, i+1)

            logger.debug(f'Copying {local_tiff_path} to {grib_data.bucket_path}')
            with open(local_tiff_path, 'rb') as src:
                with FileSystems.create(grib_data.bucket_path) as dst:
                    shutil.copyfileobj(src, dst, DEFAULT_READ_BUFFER_SIZE)

            manifest = {
                'name': grib_data.asset_id,
                'tilesets': [{
                    'id': grib_data.name,
                    'crs': "epsg:4326",
                    'sources': [{
                        'uris': [grib_data.bucket_path]
                    }]
                }],
                'bands': [{
                    'id': da.name,
                    'tileset_id': grib_data.name,
                    'tileset_band_index': i
                } for i, da in enumerate(grib_data.data)],
                'start_time': {
                    'seconds': grib_data.start_time
                },
                'end_time': {
                    'seconds': grib_data.end_time
                },
                'properties': grib_data.metadata
            }
            task_id = ee.data.newTaskId(1)[0]
            logger.info(f'Ingesting {grib_data.name} into EE with task id: {task_id}')
            _ = ee.data.startIngestion(task_id, manifest)

            yield task_id
