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
import ee
import json
import logging
import numpy as np
import os
import re
import shutil
import subprocess
import tempfile
import time
import typing as t
import xarray as xr

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.gcsio import WRITE_CHUNK_SIZE
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils import retry
from google.auth import compute_engine, default, credentials
from google.auth.transport import requests
from rasterio.io import MemoryFile

from .sinks import ToDataSink, open_dataset, open_local
from .util import RateLimit, validate_region

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

COMPUTE_ENGINE_STR = 'Metadata-Flavor: Google'
# For EE ingestion retry logic.
INITIAL_DELAY = 1.0  # Initial delay in seconds.
MAX_DELAY = 600  # Maximum delay before giving up in seconds.
NUM_RETRIES = 10  # Number of tries with exponential backoff.
TASK_QUEUE_WAIT_TIME = 120  # Task queue wait time in seconds.


def is_compute_engine() -> bool:
    """Determines if the application in running in Compute Engine Environment."""
    command = ['curl', 'metadata.google.internal', '-i']
    result = subprocess.run(command, stdout=subprocess.PIPE)
    result_output = result.stdout.decode('utf-8')
    return COMPUTE_ENGINE_STR in result_output


def get_creds(use_personal_account: bool, service_account: str, private_key: str) -> credentials.Credentials:
    """Fetches credentials for authentication.

    If the `use_personal_account` argument is true then it will authenticate with pop-up
    browser window using personal account. Otherwise, if the application is running
    in compute engine, it will use credentials of service account bound to the VM.
    Otherwise, it will try to use user credentials.

    Args:
        use_personal_account: A flag to use personal account for ee authentication.
        service_account: Service account address when using a private key for earth engine authentication.
        private_key: A private key path to authenticate earth engine using private key.

    Returns:
        cred: Credentials object.
    """
    # TODO(Issue #197): Test private key authentication.
    if service_account and private_key:
        try:
            with open_local(private_key) as local_path:
                creds = ee.ServiceAccountCredentials(service_account, local_path)
        except Exception:
            raise RuntimeError(f'Unable to open the private key {private_key}.')
    elif use_personal_account:
        ee.Authenticate()
        creds, _ = default()
    elif is_compute_engine():
        creds = compute_engine.Credentials()
    else:
        creds, _ = default()

    creds.refresh(requests.Request())
    return creds


def ee_initialize(use_personal_account: bool = False,
                  enforce_high_volume: bool = False,
                  service_account: t.Optional[str] = None,
                  private_key: t.Optional[str] = None) -> None:
    """Initializes earth engine with the high volume API when using a compute engine VM.

    Args:
        use_personal_account: A flag to use personal account for ee authentication. Default: False.
        enforce_high_volume: A flag to use the high volume API when using a compute engine VM. Default: False.
        service_account: Service account address when using a private key for earth engine authentication.
        private_key: A private key path to authenticate earth engine using private key. Default: None.

    Raises:
        RuntimeError: Earth Engine did not initialize.
    """
    creds = get_creds(use_personal_account, service_account, private_key)
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


class SetupEarthEngine(RateLimit):
    """A base class to setup the earth engine."""

    def __init__(self,
                 ee_qps: int,
                 ee_latency: float,
                 ee_max_concurrent: int,
                 private_key: str,
                 service_account: str,
                 use_personal_account: bool):
        super().__init__(global_rate_limit_qps=ee_qps,
                         latency_per_request=ee_latency,
                         max_concurrent_requests=ee_max_concurrent)
        self._has_setup = False
        self.private_key = private_key
        self.service_account = service_account
        self.use_personal_account = use_personal_account

    def setup(self):
        """Makes sure ee is set up on every worker."""
        ee_initialize(use_personal_account=self.use_personal_account,
                      service_account=self.service_account,
                      private_key=self.private_key)
        self._has_setup = True

    def check_setup(self):
        """Ensures that setup has been called."""
        if not self._has_setup:
            self.setup()

    def process(self, *args, **kwargs):
        """Checks that setup has been called then call the process implementation."""
        self.check_setup()


def get_ee_safe_name(uri: str) -> str:
    """Extracts file name and converts it into an EE-safe name"""
    basename = os.path.basename(uri)
    # Strip the extension from the basename.
    basename, _ = os.path.splitext(basename)
    # An asset ID can only contain letters, numbers, hyphens, and underscores.
    # Converting everything else to underscore.
    asset_name = re.sub(r'[^a-zA-Z0-9-_]+', r'_', basename)
    return asset_name


@dataclasses.dataclass
class AssetData:
    """A class for holding the asset data.

    Attributes:
        name: The EE-safe name of the asset.
        target_path: The location of the asset in GCS.
        channel_names: A list of channel names in the asset.
        start_time: Image start time in floating point seconds since epoch.
        end_time: Image end time in floating point seconds since epoch.
        properties: A dictionary of asset metadata.
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

    A sink that loads dataset (either normalized or read using user-provided kwargs).
    This sink will read each channel data and merge them into a single dataset if the
    `disable_grib_schema_normalization` flag is not specified. It will read the
    dataset and create an asset. Next, it will write the asset to the specified
    bucket path and initiate the earth engine upload request.

    When using the default service account bound to the VM, it is required to register the
    service account with EE from `here`_. See `this doc`_ for more detail.

    Attributes:
        asset_location: The bucket location at which asset files will be pushed.
        ee_asset: The asset folder path in earth engine project where the asset files will be pushed.
        ee_asset_type: The type of asset to ingest in the earth engine. Default: IMAGE.
        xarray_open_dataset_kwargs: A dictionary of kwargs to pass to xr.open_dataset().
        disable_grib_schema_normalization: A flag to turn grib schema normalization off; Default: on.
        skip_region_validation: Turn off validation that checks if all Cloud resources
          are in the same region.
        use_personal_account: A flag to authenticate earth engine using personal account. Default: False.

    .. _here: https://signup.earthengine.google.com/#!/service_accounts
    .. _this doc: https://developers.google.com/earth-engine/guides/service_account
    """
    asset_location: str
    ee_asset: str
    ee_asset_type: str
    xarray_open_dataset_kwargs: t.Dict
    disable_grib_schema_normalization: bool
    skip_region_validation: bool
    use_personal_account: bool
    service_account: str
    private_key: str
    ee_qps: int
    ee_latency: float
    ee_max_concurrent: int

    @classmethod
    def add_parser_arguments(cls, subparser: argparse.ArgumentParser):
        subparser.add_argument('--asset_location', type=str, required=True, default=None,
                               help='The GCS location where the asset files will be pushed.')
        subparser.add_argument('--ee_asset', type=str, required=True, default=None,
                               help='The asset folder path in earth engine project where the asset files'
                               ' will be pushed.')
        subparser.add_argument('--ee_asset_type', type=str, choices=['IMAGE', 'TABLE'], default='IMAGE',
                               help='The type of asset to ingest in the earth engine.')
        subparser.add_argument('--xarray_open_dataset_kwargs', type=json.loads, default='{}',
                               help='Keyword-args to pass into `xarray.open_dataset()` in the form of a JSON string.')
        subparser.add_argument('--disable_grib_schema_normalization', action='store_true', default=False,
                               help='To disable merge of grib datasets. Default: False')
        subparser.add_argument('-s', '--skip-region-validation', action='store_true', default=False,
                               help='Skip validation of regions for data migration. Default: off')
        subparser.add_argument('-u', '--use_personal_account', action='store_true', default=False,
                               help='To use personal account for earth engine authentication.')
        subparser.add_argument('--service_account', type=str, default=None,
                               help='Service account address when using a private key for earth engine authentication.')
        subparser.add_argument('--private_key', type=str, default=None,
                               help='To use a private key for earth engine authentication.')
        subparser.add_argument('--ee_qps', type=int, default=10,
                               help='Maximum queries per second allowed by EE for your project. Default: 10')
        subparser.add_argument('--ee_latency', type=float, default=0.5,
                               help='The expected latency per requests, in seconds. Default: 0.5')
        subparser.add_argument('--ee_max_concurrent', type=int, default=10,
                               help='Maximum concurrent api requests to EE allowed for your project. Default: 10')

    @classmethod
    def validate_arguments(cls, known_args: argparse.Namespace, pipeline_args: t.List[str]) -> None:
        pipeline_options = PipelineOptions(pipeline_args)
        pipeline_options_dict = pipeline_options.get_all_options()

        if known_args.zarr:
            raise RuntimeError('Reading Zarr is not (yet) supported.')

        # Check that ee_asset is in correct format.
        if not re.match("^projects/.+/assets.*", known_args.ee_asset):
            raise RuntimeError("'--ee_asset' is required to be in format: projects/+/assets/*.")

        # Check that both service_account and private_key are provided, or none is.
        if bool(known_args.service_account) ^ bool(known_args.private_key):
            raise RuntimeError("'--service_account' and '--private_key' both are required.")

        # Check that either personal or service account is asked to use.
        if known_args.use_personal_account and known_args.service_account:
            raise RuntimeError("Both personal and service account cannot be used at once.")

        if known_args.ee_qps and known_args.ee_qps < 1:
            raise RuntimeError("Queries per second should not be less than 1.")

        if known_args.ee_latency and known_args.ee_latency < 0.001:
            raise RuntimeError("Latency per request should not be less than 0.001.")

        if known_args.ee_max_concurrent and known_args.ee_max_concurrent < 1:
            raise RuntimeError("Maximum concurrent requests should not be less than 1.")

        # Check that Cloud resource regions are consistent.
        if not (known_args.dry_run or known_args.skip_region_validation):
            # Program execution will terminate on failure of region validation.
            logger.info('Validating regions for data migration. This might take a few seconds...')
            validate_region(temp_location=pipeline_options_dict.get('temp_location'),
                            region=pipeline_options_dict.get('region'))
            logger.info('Region validation completed successfully.')

    def expand(self, paths):
        """Converts input data files into assets and uploads them into the earth engine."""
        if not self.dry_run:
            (
                paths
                | 'FilterFiles' >> FilterFilesTransform(
                    ee_asset=self.ee_asset,
                    ee_qps=self.ee_qps,
                    ee_latency=self.ee_latency,
                    ee_max_concurrent=self.ee_max_concurrent,
                    private_key=self.private_key,
                    service_account=self.service_account,
                    use_personal_account=self.use_personal_account)
                | 'ReshuffleFiles' >> beam.Reshuffle()
                | 'ConvertToAsset' >> beam.ParDo(
                    ConvertToAsset(
                        asset_location=self.asset_location,
                        ee_asset_type=self.ee_asset_type,
                        open_dataset_kwargs=self.xarray_open_dataset_kwargs,
                        disable_grib_schema_normalization=self.disable_grib_schema_normalization))
                | 'IngestIntoEE' >> IngestIntoEETransform(
                    ee_asset=self.ee_asset,
                    ee_asset_type=self.ee_asset_type,
                    ee_qps=self.ee_qps,
                    ee_latency=self.ee_latency,
                    ee_max_concurrent=self.ee_max_concurrent,
                    private_key=self.private_key,
                    service_account=self.service_account,
                    use_personal_account=self.use_personal_account)
            )
        else:
            (
                paths
                | 'Log Files' >> beam.Map(logger.info)
            )


class FilterFilesTransform(SetupEarthEngine):
    """Filters out paths for which the assets that are already in the earth engine.

    Attributes:
        ee_asset: The asset folder path in earth engine project where the asset files will be pushed.
        ee_qps: Maximum queries per second allowed by EE for your project.
        ee_latency: The expected latency per requests, in seconds.
        ee_max_concurrent: Maximum concurrent api requests to EE allowed for your project.
        private_key: A private key path to authenticate earth engine using private key. Default: None.
        service_account: Service account address when using a private key for earth engine authentication.
        use_personal_account: A flag to authenticate earth engine using personal account. Default: False.
    """

    def __init__(self,
                 ee_asset: str,
                 ee_qps: int,
                 ee_latency: float,
                 ee_max_concurrent: int,
                 private_key: str,
                 service_account: str,
                 use_personal_account: bool):
        """Sets up rate limit and initializes the earth engine."""
        super().__init__(ee_qps=ee_qps,
                         ee_latency=ee_latency,
                         ee_max_concurrent=ee_max_concurrent,
                         private_key=private_key,
                         service_account=service_account,
                         use_personal_account=use_personal_account)
        self.ee_asset = ee_asset

    def process(self, uri: str) -> t.Iterator[str]:
        """Yields uri if the asset does not already exist."""
        self.check_setup()

        asset_name = get_ee_safe_name(uri)
        asset_id = os.path.join(self.ee_asset, asset_name)
        try:
            ee.data.getAsset(asset_id)
            logger.info(f'Asset {asset_id} already exists in EE. Skipping...')
        except ee.EEException:
            yield uri


@dataclasses.dataclass
class ConvertToAsset(beam.DoFn):
    """Writes asset after extracting input data and uploads it to GCS.

    Attributes:
        ee_asset_type: The type of asset to ingest in the earth engine. Default: IMAGE.
        asset_location: The bucket location at which asset files will be pushed.
        open_dataset_kwargs: A dictionary of kwargs to pass to xr.open_dataset().
        disable_grib_schema_normalization: A flag to turn grib schema normalization off; Default: on.
    """

    asset_location: str
    ee_asset_type: str = 'IMAGE'
    open_dataset_kwargs: t.Optional[t.Dict] = None
    disable_grib_schema_normalization: bool = False

    def process(self, uri: str) -> t.Iterator[AssetData]:
        """Opens grib files and yields AssetData."""

        logger.info(f'Converting {uri!r} to COGs...')
        with open_dataset(uri,
                          self.open_dataset_kwargs,
                          self.disable_grib_schema_normalization) as ds:

            attrs = ds.attrs
            data = list(ds.values())
            asset_name = get_ee_safe_name(uri)
            channel_names = [da.name for da in data]
            start_time, end_time, is_normalized = (attrs.get(key) for key in
                                                   ('start_time', 'end_time', 'is_normalized'))
            dtype, crs, transform = (attrs.pop(key) for key in ['dtype', 'crs', 'transform'])
            attrs.update({'is_normalized': str(is_normalized)})  # EE properties does not support bool.

            # For tiff ingestions.
            if self.ee_asset_type == 'IMAGE':
                file_name = f'{asset_name}.tiff'

                with MemoryFile() as memfile:
                    with memfile.open(driver='COG',
                                      dtype=dtype,
                                      width=data[0].data.shape[1],
                                      height=data[0].data.shape[0],
                                      count=len(data),
                                      nodata=np.nan,
                                      crs=crs,
                                      transform=transform,
                                      compress='lzw') as f:
                        for i, da in enumerate(data):
                            f.write(da, i+1)
                            # Making the channel name EE-safe before adding it as a band name.
                            f.set_band_description(i+1, get_ee_safe_name(channel_names[i]))
                            f.update_tags(i+1, band_name=channel_names[i])

                        # Write attributes as tags in tiff.
                        f.update_tags(**attrs)

                    # Copy in-memory tiff to gcs.
                    target_path = os.path.join(self.asset_location, file_name)
                    with FileSystems().create(target_path) as dst:
                        shutil.copyfileobj(memfile, dst, WRITE_CHUNK_SIZE)
            # For feature collection ingestions.
            elif self.ee_asset_type == 'TABLE':
                file_name = f'{asset_name}.csv'

                df = xr.Dataset.to_dataframe(ds)
                df = df.reset_index()

                # Copy in-memory dataframe to gcs.
                target_path = os.path.join(self.asset_location, file_name)
                with tempfile.NamedTemporaryFile() as tmp_df:
                    df.to_csv(tmp_df.name, index=False)
                    tmp_df.flush()
                    tmp_df.seek(0)
                    with FileSystems().create(target_path) as dst:
                        shutil.copyfileobj(tmp_df, dst, WRITE_CHUNK_SIZE)

            asset_data = AssetData(
                name=asset_name,
                target_path=target_path,
                channel_names=[],
                start_time=start_time,
                end_time=end_time,
                properties=attrs
            )

            yield asset_data


class IngestIntoEETransform(SetupEarthEngine):
    """Ingests asset into earth engine and yields asset id.

    Attributes:
        ee_asset: The asset folder path in earth engine project where the asset files will be pushed.
        ee_asset_type: The type of asset to ingest in the earth engine. Default: IMAGE.
        ee_qps: Maximum queries per second allowed by EE for your project.
        ee_latency: The expected latency per requests, in seconds.
        ee_max_concurrent: Maximum concurrent api requests to EE allowed for your project.
        private_key: A private key path to authenticate earth engine using private key. Default: None.
        service_account: Service account address when using a private key for earth engine authentication.
        use_personal_account: A flag to authenticate earth engine using personal account. Default: False.
    """

    def __init__(self,
                 ee_asset: str,
                 ee_asset_type: str,
                 ee_qps: int,
                 ee_latency: float,
                 ee_max_concurrent: int,
                 private_key: str,
                 service_account: str,
                 use_personal_account: bool):
        """Sets up rate limit."""
        super().__init__(ee_qps=ee_qps,
                         ee_latency=ee_latency,
                         ee_max_concurrent=ee_max_concurrent,
                         private_key=private_key,
                         service_account=service_account,
                         use_personal_account=use_personal_account)
        self.ee_asset = ee_asset
        self.ee_asset_type = ee_asset_type

    def ee_tasks_remaining(self) -> int:
        """Returns the remaining number of tasks in the tassk queue of earth engine."""
        return len([task for task in ee.data.getTaskList()
                    if task['state'] in ['UNSUBMITTED', 'READY', 'RUNNING']])

    def wait_for_task_queue(self) -> None:
        """Waits until the task queue has space.

        Ingestion of table in the earth engine creates a task and every project has a limited task queue size. This
        function checks the task queue size and waits until the task queue has some space.
        """
        while self.ee_tasks_remaining() >= self._num_shards:
            time.sleep(TASK_QUEUE_WAIT_TIME)

    @retry.with_exponential_backoff(
        num_retries=NUM_RETRIES,
        logger=logger.warning,
        initial_delay_secs=INITIAL_DELAY,
        max_delay_secs=MAX_DELAY
    )
    def start_ingestion(self, asset_request: t.Dict) -> str:
        """Creates COG-backed asset in earth engine. Returns the asset id."""
        self.check_setup()

        try:
            if self.ee_asset_type == 'IMAGE':
                result = ee.data.createAsset(asset_request)
            elif self.ee_asset_type == 'TABLE':
                self.wait_for_task_queue()
                task_id = ee.data.newTaskId(1)[0]
                result = ee.data.startTableIngestion(task_id, asset_request)
        except ee.EEException as e:
            logger.error(f"Failed to create asset '{asset_request['name']}' in earth engine: {e}")
            # We do have logic for skipping the already created assets in FilterFilesTransform but
            # somehow we are observing that streaming pipeline reports "Cannot overwrite ..." error
            # so this will act as a quick fix for this issue.
            if f"Cannot overwrite asset '{asset_request['name']}'" in repr(e):
                ee.data.deleteAsset(asset_request['name'])
            raise

        return result.get('id')

    def process(self, asset_data: AssetData) -> t.Iterator[str]:
        """Uploads an asset into the earth engine."""
        asset_name = os.path.join(self.ee_asset, asset_data.name)

        request = {
            'name': asset_name,
            'startTime': asset_data.start_time,
            'endTime': asset_data.end_time,
            'properties': asset_data.properties
        }

        # Add uris.
        uris_as_per_asset_type = {
            'IMAGE': {
                'type': self.ee_asset_type,
                'gcs_location': {
                    'uris': [asset_data.target_path]
                }
            },
            'TABLE': {
                'sources': [{
                    'uris': [asset_data.target_path]
                }]
            }
        }
        request.update(uris_as_per_asset_type[self.ee_asset_type])

        logger.info(f"Uploading asset {asset_data.target_path} to Asset ID '{asset_name}'.")
        asset_id = self.start_ingestion(request)

        beam.metrics.Metrics.counter('Success', 'IngestIntoEE').inc()

        yield asset_id
