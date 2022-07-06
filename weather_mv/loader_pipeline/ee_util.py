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
import signal
import sys
import dataclasses
import logging
import rasterio
import tempfile
import traceback
import typing as t
import uuid
from functools import partial
from urllib.parse import urlparse
import ee
import xarray as xr
import subprocess
from retry import retry
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.api_core.exceptions import BadRequest
from google.api_core.exceptions import NotFound
from google.cloud import storage
from google.auth import compute_engine, default
from google.auth.transport import requests

from .sinks import ToDataSink, open_dataset, open_local

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CANARY_BUCKET_NAME = 'anthromet_canary_bucket'
CANARY_RECORD = {'foo': 'bar'}
CANARY_RECORD_FILE_NAME = 'canary_record.json'


def is_compute_engine():
    """
    Determines if the application in running in Compute Engine Environment

    Returns:
    a bool indicating whether the application is running in compute engine
    """
    COMPUTE_ENGINE_STR = 'Metadata-Flavor: Google'
    command = ['curl', 'metadata.google.internal', '-i']
    result = subprocess.run(command, stdout=subprocess.PIPE)
    result_output = result.stdout.decode('utf-8')
    return COMPUTE_ENGINE_STR in result_output


# Concurrent calls to OAuth can cause problems. See yaqs/8220221608137064448.
def get_creds(use_personal_account):
    """
    Fetches credentials for authentication

    Note: If the application is running in compute engine, it will use credentials
    of service account bound to the VM otherwise will try to use user credentials.

    Returns:
        credentials
    """
    # Authenticate with pop-up if credentials are not available
    if use_personal_account:
        ee_cred_location = os.path.join(os.path.expanduser('~'), '.config/earthengine/credentials')
        if not os.path.exists(ee_cred_location):
            ee.Authenticate()

    if is_compute_engine() and not use_personal_account:
        creds = compute_engine.Credentials()
    else:
        creds, _ = default()
    request = requests.Request()
    creds.refresh(request)
    return creds


def ee_initialize(use_personal_account=False,
                  enforce_high_volume=False,
                  allow_old_versions=False):
    """Initializes ee with the high volume API when using a compute engine VM.

    Once earth engine is initialized, repeated calls will still verify the high
    volume api and old version constrains, but will then no-op.

    Raises:
        RuntimeError: Earth Engine did not initialize.
    """
    creds = get_creds(use_personal_account)

    # Only the compute engine service service account can access the high volume api.
    # The high volume api is only available with ee version 0.1.263 or later.

    up_to_date = ee.__version__ >= '0.1.263'
    on_compute_engine = is_compute_engine()

    # Try to use the high volume api
    if up_to_date and on_compute_engine:
        ee.Initialize(creds, opt_url='https://earthengine-highvolume.googleapis.com')

    # If the high volume api is required, find out why
    elif enforce_high_volume and not on_compute_engine:
        raise RuntimeError(
            'Must run on a compute engine VM to use the high volume earth engine api.'
        )
    elif enforce_high_volume and not up_to_date:
        raise RuntimeError(
            'Earth engine api must be updated to version ≥0.1.263 to use the high volume api'
        )

    # Ensure the api version is acceptable
    elif not up_to_date and not allow_old_versions:
        raise RuntimeError(
            'Earth engine api is out of date. Update to version ≥0.1.263.')

    # Try using the service account
    elif on_compute_engine:
        ee.Initialize(creds)

    # Try using an personal authentication
    else:
        ee.Initialize()


@dataclasses.dataclass
class GribData(object):
    """A class for holding the output of the GetChannelsDoFn."""
    name: str  # The EE-safe name of the channel.
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
    skip_region_validation: bool
    ee_asset: str
    tiff_location: str
    use_personal_account: bool

    @classmethod
    def add_parser_arguments(cls, subparser: argparse.ArgumentParser):
        subparser.add_argument('--xarray_open_dataset_kwargs', type=json.loads, default='{}',
                               help='Keyword-args to pass into `xarray.open_dataset()` in the form of a JSON string.')
        subparser.add_argument('--disable_in_memory_copy', action='store_true', default=False,
                               help='To disable in-memory copying of dataset. Default: False')
        subparser.add_argument('--disable_grib_schema_normalization', action='store_true', default=False,
                               help='To disable merge of grib datasets. Default: False')
        subparser.add_argument('--tif_metadata_for_datetime', type=str, default=None,
                               help='Metadata that contains tif file\'s timestamp. '
                                    'Applicable only for tif files.')
        subparser.add_argument('--ee_asset', type=str, default=None,
                               help='The name of the asset folder you want to put the images in.')
        subparser.add_argument('-s', '--skip-region-validation', action='store_true', default=False,
                               help='Skip validation of regions for data migration. Default: off')
        subparser.add_argument('--tiff_location', type=str, default=None,
                               help='The name of the GCS location where the tiff file will be stored.')
        subparser.add_argument('--use_personal_account', action='store_true', default=False,
                               help='To use personal account for earth engine authentication.')

    @classmethod
    def validate_arguments(cls, known_args: argparse.Namespace, pipeline_args: t.List[str]) -> None:
        pipeline_options = PipelineOptions(pipeline_args)
        pipeline_options_dict = pipeline_options.get_all_options()

        if not known_args.tiff_location:
            raise RuntimeError("'--tiff_location' is required for ee ingestion.")
        if not known_args.ee_asset:
            raise RuntimeError("'--ee_asset' is required for ee ingestion.")
        if not re.match("^projects/.+/assets.*", known_args.ee_asset):
            raise RuntimeError("'--ee_asset' is required to be in format: projects/+/assets/*.")

        # Check that Cloud resource regions are consistent.
        if not (known_args.dry_run or known_args.skip_region_validation):
            # Program execution will terminate on failure of region validation.
            logger.info('Validating regions for data migration. This might take a few seconds...')
            validate_region(temp_location=pipeline_options_dict.get('temp_location'),
                            region=pipeline_options_dict.get('region'))
            logger.info('Region validation completed successfully.')

    def __post_init__(self):
        """Initializes Sink by initialize earthengine based on user input."""

        if self.dry_run:
            logger.debug('Uploaded assets into earth engine ...')
            return

    def expand(self, paths):
        """Convert gribs from paths into tiff and upload them into earth engine."""
        (
            paths
            | 'ExtractGribData' >> beam.ParDo(
                ExtractGribData(
                    open_dataset_kwargs=self.xarray_open_dataset_kwargs,
                    disable_in_memory_copy=self.disable_in_memory_copy,
                    disable_grib_schema_normalization=self.disable_grib_schema_normalization,
                    tif_metadata_for_datetime=self.tif_metadata_for_datetime))
            | 'IngestIntoEE' >> beam.ParDo(
                IngestIntoEE(
                    ee_asset=self.ee_asset,
                    tiff_location=self.tiff_location,
                    use_personal_account=self.use_personal_account))
        )


@dataclasses.dataclass
class ExtractGribData(beam.DoFn):
    """Extract grib data from merged dataset."""

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
            start_time, end_time = (attrs.get(key) for key in ('start_time', 'end_time'))

            grib_data = GribData(
                    grib_name,
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

    def __init__(self, ee_asset, tiff_location, use_personal_account):
        self.ee_asset = ee_asset
        self.tiff_location = tiff_location
        self.use_personal_account = use_personal_account

    @retry(tries=2, delay=10, backoff=2)
    def start_ingestion_task(self, asset_request):
        """Sends a manifest task to EE for asset ingestion. Returns the task id."""
        task_id = ee.data.newTaskId(1)[0]
        _ = ee.data.startIngestion(task_id, asset_request)
        return task_id

    def manifest_output(self,
                        out_path: str,
                        asset_id: str,
                        variable_list: t.List[str],
                        start_time: float,
                        end_time: float,
                        properties: t.Dict[str, t.Union[str, float, int]]) -> str:
        """Uploads a GeoTiff to EE for weather data.

        This function uses the "Manifest Upload" technique to move a GeoTiff from GCS
        to EE. This technique is advantageous over ee.Export because it allows for
        uploads of very large files, but requires the generation of a manifest JSON.

        Args:
            out_path: The location of the GeoTiff in GCS
            ee_collection: earthengine collection path.
            variable_list: list of variables that will be ingested in single asset.
            start_time: dataset start time in float seconds
            end_time: dataset end time in float seconds
            properties: metadata properties

        Returns:
            The Task ID of the EE ingestion request
        """
        manifest = {
            'name': asset_id,
            'tilesets': [{
                'id': 'data_tile',
                'crs': "epsg:4326",
                'sources': [{
                    'uris': [out_path]
                }]
            }],
            'bands': [{
                'id': f"{variable}",
                'tileset_id': 'data_tile',
                'tileset_band_index': idx,
            } for idx, variable in enumerate(variable_list)],
            'start_time': {
                'seconds': start_time
            },
            'end_time': {
                'seconds': end_time
            },
            'properties': properties
        }

        logger.info(f"Uploading GeoTiff {out_path} to Asset ID {asset_id}.")
        task_id = self.start_ingestion_task(manifest)
        return task_id

    def process(self, grib_data: GribData) -> t.Iterator[str]:
        """Writes tiff file, upload it to bucket and ingest into earth engine"""

        # Initializing earth engine
        ee_initialize(use_personal_account=self.use_personal_account)

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

            # Copy local tiff to gcs tiff location
            storage_client = storage.Client()
            parsed_tiff_location = urlparse(self.tiff_location)
            bucket_name = parsed_tiff_location.netloc
            bucket = storage_client.get_bucket(bucket_name)

            tiff_internal_path = str(parsed_tiff_location.path)
            tiff_internal_path = tiff_internal_path[1:] if tiff_internal_path.startswith('/') else tiff_internal_path
            tiff_bucket_path = os.path.join(tiff_internal_path, file_name)

            logger.debug(f'Copying {local_tiff_path} to {tiff_bucket_path}')
            blob = bucket.blob(tiff_bucket_path)
            blob.upload_from_filename(local_tiff_path)

            # Push into earth engine
            out_path = tiff_bucket_path
            asset_id = os.path.join(self.ee_asset, grib_data.name)
            variable_list = [da.name for da in grib_data.data]
            start_time = grib_data.start_time
            end_time = grib_data.end_time
            properties = grib_data.metadata

            task_id = self.manifest_output(out_path, asset_id, variable_list, start_time, end_time, properties)
            yield task_id


def _cleanup(storage_client: storage.Client,
             canay_bucket_name: str,
             sig: t.Optional[t.Any] = None,
             frame: t.Optional[t.Any] = None) -> None:
    """Cleaning up the bucket."""
    try:
        storage_client.get_bucket(canay_bucket_name).delete(force=True)
    except NotFound:
        pass
    if sig:
        traceback.print_stack(frame)
        sys.exit(0)


def validate_region(temp_location: t.Optional[str] = None, region: t.Optional[str] = None) -> None:
    """Validates non-compatible regions scenarios by performing sanity check."""
    if not region and not temp_location:
        raise ValueError('Invalid GCS location: None.')

    storage_client = storage.Client()
    canary_bucket_name = CANARY_BUCKET_NAME + str(uuid.uuid4())

    # Doing cleanup if operation get cut off midway.
    # TODO : Should we handle some other signals ?
    do_cleanup = partial(_cleanup, storage_client, canary_bucket_name)
    original_sigint_handler = signal.getsignal(signal.SIGINT)
    original_sigtstp_handler = signal.getsignal(signal.SIGTSTP)
    signal.signal(signal.SIGINT, do_cleanup)
    signal.signal(signal.SIGTSTP, do_cleanup)

    bucket_region = region

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
    except BadRequest:
        raise RuntimeError(f'Can\'t write to destination: {bucket_region}')
    finally:
        _cleanup(storage_client, canary_bucket_name)
        signal.signal(signal.SIGINT, original_sigint_handler)
        signal.signal(signal.SIGINT, original_sigtstp_handler)
