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
import json
import os
import dataclasses
import logging
import rasterio
import tempfile
import typing as t
import ee
import subprocess
import re
import shutil

import apache_beam as beam
from apache_beam.utils import retry
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.gcsio import WRITE_CHUNK_SIZE
from apache_beam.options.pipeline_options import PipelineOptions
from google.auth import compute_engine, default, credentials
from google.auth.transport import requests

from .sinks import ToDataSink, open_dataset
from .util import validate_region

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CANARY_BUCKET_NAME = 'anthromet_canary_bucket'
CANARY_RECORD = {'foo': 'bar'}
CANARY_RECORD_FILE_NAME = 'canary_record.json'


def is_compute_engine() -> bool:
    """Determines if the application in running in Compute Engine Environment."""
    COMPUTE_ENGINE_STR = 'Metadata-Flavor: Google'
    command = ['curl', 'metadata.google.internal', '-i']
    result = subprocess.run(command, stdout=subprocess.PIPE)
    result_output = result.stdout.decode('utf-8')
    return COMPUTE_ENGINE_STR in result_output


def get_creds(use_personal_account: bool) -> credentials.Credentials:
    """Fetches credentials for authentication.

    If the `use_personal_account` argument is true then it will authenticate with pop-up
    browser window using personal account. Otherwise if the application is running
    in compute engine, it will use credentials of service account bound to the VM
    otherwise will try to use user credentials.
    """
    if use_personal_account:
        ee.Authenticate()

    if not use_personal_account and is_compute_engine():
        creds = compute_engine.Credentials()
    else:
        creds, _ = default()
    creds.refresh(requests.Request())
    return creds


def ee_initialize(use_personal_account: bool = False,
                  enforce_high_volume: bool = False) -> None:
    """Initializes earth engine with the high volume API when using a compute engine VM.

    Args:
        use_personal_account: A flag to use personal account for ee authentication. Default: False.
        enforce_high_volume: A flag to use the high volume API when using a compute engine VM. Default: False.

    Raises:
        RuntimeError: Earth Engine did not initialize.
    """
    creds = get_creds(use_personal_account)
    on_compute_engine = is_compute_engine()

    # Using the high volume api.
    if on_compute_engine:
        ee.Initialize(creds, opt_url='https://earthengine-highvolume.googleapis.com')

    # Only the compute engine service service account can access the high volume api.
    elif enforce_high_volume and not on_compute_engine:
        raise RuntimeError(
            'Must run on a compute engine VM to use the high volume earth engine api.'
        )
    else:
        ee.Initialize(creds)


@dataclasses.dataclass
class TiffData():
    """A class for holding the tiff data.

    Attributes:
        name: The EE-safe name of the tiff image.
        target_path: The location of the GeoTiff in GCS.
        channel_names: A list of channel names in tiff image.
        start_time: Image start time in floating point seconds since epoch.
        end_time: Image end time in floating point seconds since epoch.
        properties: A dictionary of tiff metadata.
    """
    name: str
    target_path: str
    channel_names: t.List[str]
    start_time: float
    end_time: float
    properties: t.Dict[str, t.Union[str, float, int]]


@dataclasses.dataclass
class ToEarthEngine(ToDataSink):
    """Loads weather data into Google Earth Engine.

    A sink that loads dataset (either normalized or read using user provided kwargs).
    This sink will read each channel data and merge them into a single dataset if the
    `disable_grib_schema_normalization` flag is not specified. It will read the
    dataset and create a tiff image. Next, it will write the tiff image to the specified
    bucket path and initiate the earth engine upload request.

    When using the default service account bound to the VM, it is required to register the
    service account with EE from `here`_. See `this doc`_ for more detail.

    Attributes:
        tiff_location: The bucket location at which tiff files will be pushed.
        ee_asset: The asset folder path in earth engine project where the tiff image files will be pushed.
        xarray_open_dataset_kwargs: A dictionary of kwargs to pass to xr.open_dataset().
        disable_in_memory_copy: A flag to turn in-memory copy off; Default: on.
        disable_grib_schema_normalization: A flag to turn grib schema normalization off; Default: on.
        skip_region_validation: Turn off validation that checks if all Cloud resources
          are in the same region.
        use_personal_account: A flag to authenticate earth engine using personal account. Default: False.

    .. _here: https://signup.earthengine.google.com/#!/service_accounts
    .. _this doc: https://developers.google.com/earth-engine/guides/service_account
    """
    tiff_location: str
    ee_asset: str
    xarray_open_dataset_kwargs: t.Dict
    disable_in_memory_copy: bool
    disable_grib_schema_normalization: bool
    skip_region_validation: bool
    use_personal_account: bool

    @classmethod
    def add_parser_arguments(cls, subparser: argparse.ArgumentParser):
        subparser.add_argument('--tiff_location', type=str, required=True, default=None,
                               help='The GCS location where the tiff files will be pushed.')
        subparser.add_argument('--ee_asset', type=str, required=True, default=None,
                               help='The asset folder path in earth engine project where the tiff image files'
                               ' will be pushed.')
        subparser.add_argument('--xarray_open_dataset_kwargs', type=json.loads, default='{}',
                               help='Keyword-args to pass into `xarray.open_dataset()` in the form of a JSON string.')
        subparser.add_argument('--disable_in_memory_copy', action='store_true', default=False,
                               help='To disable in-memory copying of dataset. Default: False')
        subparser.add_argument('--disable_grib_schema_normalization', action='store_true', default=False,
                               help='To disable merge of grib datasets. Default: False')
        subparser.add_argument('-s', '--skip-region-validation', action='store_true', default=False,
                               help='Skip validation of regions for data migration. Default: off')
        subparser.add_argument('-u', '--use_personal_account', action='store_true', default=False,
                               help='To use personal account for earth engine authentication.')

    @classmethod
    def validate_arguments(cls, known_args: argparse.Namespace, pipeline_args: t.List[str]) -> None:
        pipeline_options = PipelineOptions(pipeline_args)
        pipeline_options_dict = pipeline_options.get_all_options()

        if not re.match("^projects/.+/assets.*", known_args.ee_asset):
            raise RuntimeError("'--ee_asset' is required to be in format: projects/+/assets/*.")

        # Check that Cloud resource regions are consistent.
        if not (known_args.dry_run or known_args.skip_region_validation):
            # Program execution will terminate on failure of region validation.
            logger.info('Validating regions for data migration. This might take a few seconds...')
            validate_region(temp_location=pipeline_options_dict.get('temp_location'),
                            region=pipeline_options_dict.get('region'))
            logger.info('Region validation completed successfully.')

    def expand(self, paths):
        """Converts gribs from paths into tiff and uploads them into the earth engine."""
        if not self.dry_run:
            (
                paths
                | 'ConvertToCog' >> beam.ParDo(
                    ConvertToCog(
                        tiff_location=self.tiff_location,
                        open_dataset_kwargs=self.xarray_open_dataset_kwargs,
                        disable_in_memory_copy=self.disable_in_memory_copy,
                        disable_grib_schema_normalization=self.disable_grib_schema_normalization))
                | 'IngestIntoEE' >> beam.ParDo(
                    IngestIntoEE(
                        ee_asset=self.ee_asset,
                        use_personal_account=self.use_personal_account))
            )
        else:
            (
                paths
                | 'Log Grib Files' >> beam.Map(logger.debug)
            )


@dataclasses.dataclass
class ConvertToCog(beam.DoFn):
    """Writes tiff image after extracting grib data and uploads it to GCS.

    Attributes:
        tiff_location: The bucket location at which tiff files will be pushed.
        open_dataset_kwargs: A dictionary of kwargs to pass to xr.open_dataset().
        disable_in_memory_copy: A flag to turn in-memory copy off; Default: on.
        disable_grib_schema_normalization: A flag to turn grib schema normalization off; Default: on.
    """

    tiff_location: str
    open_dataset_kwargs: t.Optional[t.Dict] = None
    disable_in_memory_copy: bool = False
    disable_grib_schema_normalization: bool = False

    def _get_tiff_name(self, uri: str) -> str:
        """Extracts file name and converts it into an EE-safe name"""
        file_name = os.path.basename(uri)
        base_name = os.path.splitext(file_name)[0]
        tiff_name = base_name.replace('.', '_')
        return tiff_name

    def _get_target_path(self, file_name: str) -> str:
        """Creates a GCS target path from the file name."""
        return os.path.join(self.tiff_location, file_name)

    def process(self, uri: str) -> t.Iterator[TiffData]:
        """Opens grib files and yields TiffData."""

        logger.info(f'Converting {uri!r} to COGs ...')
        with open_dataset(uri,
                          self.open_dataset_kwargs,
                          self.disable_in_memory_copy,
                          self.disable_grib_schema_normalization) as ds:

            attrs = ds.attrs
            data = list(ds.values())
            tiff_name = self._get_tiff_name(uri)
            start_time, end_time = (attrs.get(key) for key in ('start_time', 'end_time'))
            dtype, crs, transform = (attrs.pop(key) for key in ['dtype', 'crs', 'transform'])

            file_name = f'{tiff_name}.tiff'

            with tempfile.TemporaryDirectory() as temp_dir:
                local_tiff_path = os.path.join(temp_dir, file_name)
                with rasterio.open(local_tiff_path, 'w',
                                   driver='GTiff',
                                   dtype=dtype,
                                   width=data[0].data.shape[1],
                                   height=data[0].data.shape[0],
                                   count=len(data),
                                   crs=crs,
                                   transform=transform,
                                   tiled=True) as f:
                    for i, da in enumerate(data):
                        f.write(da, i+1)

                # Copy local tiff to gcs.
                target_path = self._get_target_path(file_name)
                with open(local_tiff_path, 'rb') as src:
                    with FileSystems().create(target_path) as dst:
                        shutil.copyfileobj(src, dst, WRITE_CHUNK_SIZE)

                channel_names = [da.name for da in data]
                tiff_data = TiffData(
                        name=tiff_name,
                        target_path=target_path,
                        channel_names=channel_names,
                        start_time=start_time,
                        end_time=end_time,
                        properties=attrs
                    )

                yield tiff_data


@dataclasses.dataclass
class IngestIntoEE(beam.DoFn):
    """Ingests tiff image into earth engine and yields task id.

    Attributes:
        ee_asset: The asset folder path in earth engine project where the tiff image files will be pushed.
        use_personal_account: A flag to authenticate earth engine using personal account. Default: False.
    """

    ee_asset: str
    use_personal_account: bool

    @retry.with_exponential_backoff()
    def start_ingestion_task(self, asset_request: t.Dict) -> str:
        """Sends a manifest task request to EE for asset ingestion. Returns the task id."""
        task_id = ee.data.newTaskId(1)[0]
        _ = ee.data.startIngestion(task_id, asset_request)
        return task_id

    def manifest_output(self,
                        target_path: str,
                        asset_id: str,
                        channel_names: t.List[str],
                        start_time: float,
                        end_time: float,
                        properties: t.Dict[str, t.Union[str, float, int]]) -> str:
        """Uploads a GeoTiff to EE for weather data.

        This function uses the "Manifest Upload" technique to move a GeoTiff from GCS
        to EE. This technique is advantageous over ee.Export because it allows for
        uploads of very large files, but requires the generation of a manifest JSON.

        Args:
            target_path: The location of the GeoTiff in GCS.
            asset_id: The asset path in earth engine where the tiff image will be pushed.
            channel_names: A list of channel names in the tiff image.
            start_time: The start time of dataset in float seconds.
            end_time: The end time of dataset in float seconds.
            properties: A dictionary of tiff metadata.

        Returns:
            task_id: The Task ID of the EE ingestion request.
        """
        manifest = {
            'name': asset_id,
            'tilesets': [{
                'id': 'data_tile',
                'crs': "epsg:4326",
                'sources': [{
                    'uris': [target_path]
                }]
            }],
            'bands': [{
                'id': f"{_channel_name}",
                'tileset_id': 'data_tile',
                'tileset_band_index': _channel_idx,
            } for _channel_idx, _channel_name in enumerate(channel_names)],
            'start_time': {
                'seconds': start_time
            },
            'end_time': {
                'seconds': end_time
            },
            'properties': properties
        }

        logger.info(f"Uploading GeoTiff {target_path} to Asset ID {asset_id}.")
        task_id = self.start_ingestion_task(manifest)
        return task_id

    def process(self, tiff_data: TiffData) -> t.Iterator[str]:
        """Uploads a tiff image into the earth engine."""
        # Initializing earth engine.
        ee_initialize(use_personal_account=self.use_personal_account)

        # Push the tiff image into earth engine.
        task_id = self.manifest_output(target_path=tiff_data.target_path,
                                       asset_id=os.path.join(self.ee_asset, tiff_data.name),
                                       channel_names=tiff_data.channel_names,
                                       start_time=tiff_data.start_time,
                                       end_time=tiff_data.end_time,
                                       properties=tiff_data.properties)
        yield task_id
