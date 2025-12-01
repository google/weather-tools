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
import csv
import dataclasses
import json
import logging
import math
import os
import re
import shutil
import subprocess
import tempfile
import time
import typing as t
from multiprocessing import Process, Queue

import apache_beam as beam
import ee
import numpy as np
from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics import metric
from apache_beam.io.gcp.gcsio import WRITE_CHUNK_SIZE
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.utils import retry
from google.auth import compute_engine, default, credentials
from google.auth.transport import requests
from google.auth.transport.requests import AuthorizedSession
from rasterio.io import MemoryFile

from .sinks import ToDataSink, open_dataset, open_local, KwargsFactoryMixin, upload
from .util import make_attrs_ee_compatible, RateLimit, validate_region, get_utc_timestamp
from .metrics import timeit, AddTimer, AddMetrics

logger = logging.getLogger(__name__)

COMPUTE_ENGINE_STR = 'Metadata-Flavor: Google'
# For EE ingestion retry logic.
INITIAL_DELAY = 1.0  # Initial delay in seconds.
MAX_DELAY = 600  # Maximum delay before giving up in seconds.
NUM_RETRIES = 10  # Number of tries with exponential backoff.
TASK_QUEUE_WAIT_TIME = 120  # Task queue wait time in seconds.
ASSET_TYPE_TO_EXTENSION_MAPPING = {
    'IMAGE': '.tiff',
    'TABLE': '.csv'
}
ROWS_PER_WRITE = 10_000  # Number of rows per feature collection write.


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
                  private_key: t.Optional[str] = None,
                  project_id: t.Optional[str] = None) -> None:
    """Initializes earth engine with the high volume API when using a compute engine VM.

    Args:
        use_personal_account: A flag to use personal account for ee authentication. Default: False.
        enforce_high_volume: A flag to use the high volume API when using a compute engine VM. Default: False.
        service_account: Service account address when using a private key for earth engine authentication.
        private_key: A private key path to authenticate earth engine using private key. Default: None.
        Project ID: An identifier that represents the name of a project present in Earth Engine.
    Raises:
        RuntimeError: Earth Engine did not initialize.
    """
    creds = get_creds(use_personal_account, service_account, private_key)
    on_compute_engine = is_compute_engine()
    # Using the high volume api.
    if on_compute_engine:
        if project_id is None and use_personal_account:
            raise RuntimeError('Project_name should not be None!')
        params = {'credentials': creds, 'opt_url': 'https://earthengine-highvolume.googleapis.com'}
        if project_id:
            params['project'] = project_id
        ee.Initialize(**params)

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
                 use_personal_account: bool,
                 use_metrics: bool):
        super().__init__(global_rate_limit_qps=ee_qps,
                         latency_per_request=ee_latency,
                         max_concurrent_requests=ee_max_concurrent,
                         use_metrics=use_metrics)
        self._has_setup = False
        self.private_key = private_key
        self.service_account = service_account
        self.use_personal_account = use_personal_account
        self.use_metrics = use_metrics

    def setup(self, project_id):
        """Makes sure ee is set up on every worker."""
        ee_initialize(use_personal_account=self.use_personal_account,
                      service_account=self.service_account,
                      private_key=self.private_key,
                      project_id=project_id)
        self._has_setup = True

    def check_setup(self, project_id: t.Optional[str] = None):
        """Ensures that setup has been called."""
        if not self._has_setup:
            try:
                # This throws an exception if ee is not initialized.
                ee.data.getAlgorithms()
                self._has_setup = True
            except ee.EEException:
                self.setup(project_id)

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
    force: bool
    service_account: str
    private_key: str
    ee_qps: int
    ee_latency: float
    ee_max_concurrent: int
    group_common_hypercubes: bool
    band_names_mapping: str
    initialization_time_regex: str
    forecast_time_regex: str
    ingest_as_virtual_asset: bool
    use_deflate: bool
    use_metrics: bool
    use_monitoring_metrics: bool
    topic: str
    # Pipeline arguments.
    job_name: str
    project: str
    region: str

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
        subparser.add_argument('-f', '--force', action='store_true', default=False,
                               help='A flag that allows overwriting of existing asset files in the GCS bucket.'
                                    ' Default: off, which means that the ingestion of URIs for which assets files'
                                    ' (GeoTiff/CSV) already exist in the GCS bucket will be skipped.')
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
        subparser.add_argument('--group_common_hypercubes', action='store_true', default=False,
                               help='To group common hypercubes into image collections when loading grib data.')
        subparser.add_argument('--band_names_mapping', type=str, default=None,
                               help='A JSON file which contains the band names for the TIFF file.')
        subparser.add_argument('--initialization_time_regex', type=str, default=None,
                               help='A Regex string to get the initialization time from the filename.')
        subparser.add_argument('--forecast_time_regex', type=str, default=None,
                               help='A Regex string to get the forecast/end time from the filename.')
        subparser.add_argument('--ingest_as_virtual_asset', action='store_true', default=False,
                               help='To ingest image as a virtual asset. Default: False')
        subparser.add_argument('--use_deflate', action='store_true', default=False,
                               help='To use deflate compression algorithm. Default: False')
        subparser.add_argument('--use_metrics', action='store_true', default=False,
                               help='If you want to add Beam metrics to your pipeline. Default: False')
        subparser.add_argument('--use_monitoring_metrics', action='store_true', default=False,
                               help='If you want to add GCP Monitoring metrics to your pipeline. Default: False')

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

        # Check that when ingesting as a virtual asset, asset type is image.
        if known_args.ingest_as_virtual_asset and known_args.ee_asset_type != "IMAGE":
            raise RuntimeError("Only assets with IMAGE type can be ingested as a virtual asset.")

        # Check that Cloud resource regions are consistent.
        if not (known_args.dry_run or known_args.skip_region_validation):
            # Program execution will terminate on failure of region validation.
            logger.info('Validating regions for data migration. This might take a few seconds...')
            validate_region(temp_location=pipeline_options_dict.get('temp_location'),
                            region=pipeline_options_dict.get('region'))
            logger.info('Region validation completed successfully.')

        # Check for the band_names_mapping json file.
        if known_args.band_names_mapping:
            if not os.path.exists(known_args.band_names_mapping):
                raise RuntimeError("--band_names_mapping file does not exist.")
            _, band_names_mapping_extension = os.path.splitext(known_args.band_names_mapping)
            if not band_names_mapping_extension == '.json':
                raise RuntimeError("--band_names_mapping should contain a json file as input.")

        # Check the initialization_time_regex and forecast_time_regex strings.
        if bool(known_args.initialization_time_regex) ^ bool(known_args.forecast_time_regex):
            raise RuntimeError("Both --initialization_time_regex & --forecast_time_regex flags need to be present")

        logger.info(f"Add metrics to pipeline: {known_args.use_metrics}")
        logger.info(f"Add Google Cloud Monitoring metrics to pipeline: {known_args.use_monitoring_metrics}")

    def expand(self, paths):
        """Converts input data files into assets and uploads them into the earth engine."""
        band_names_dict = {}
        if self.band_names_mapping:
            with open(self.band_names_mapping, 'r', encoding='utf-8') as f:
                band_names_dict = json.load(f)

        if self.use_metrics:
            paths = paths | 'AddTimer' >> beam.ParDo(AddTimer.from_kwargs(**vars(self)))

        if not self.dry_run:
            output = (
                paths
                | 'FilterFiles' >> FilterFilesTransform.from_kwargs(**vars(self))
                | 'ReshuffleFiles' >> beam.Reshuffle()
                | 'ConvertToAsset' >> beam.ParDo(
                    ConvertToAsset.from_kwargs(band_names_dict=band_names_dict, **vars(self))
                    )
                | 'IngestIntoEE' >> IngestIntoEETransform.from_kwargs(**vars(self))
            )

            if self.use_metrics:
                output | 'AddMetrics' >> AddMetrics.from_kwargs(**vars(self))
        else:
            (
                paths
                | 'Log Files' >> beam.Map(logger.info)
            )


class FilterFilesTransform(SetupEarthEngine, KwargsFactoryMixin):
    """Filters out paths for which the assets that are already in the earth engine.

    Attributes:
        ee_asset: The asset folder path in earth engine project where the asset files will be pushed.
        ee_qps: Maximum queries per second allowed by EE for your project.
        ee_latency: The expected latency per requests, in seconds.
        ee_max_concurrent: Maximum concurrent api requests to EE allowed for your project.
        force: A flag that allows overwriting of existing asset files in the GCS bucket.
        private_key: A private key path to authenticate earth engine using private key. Default: None.
        service_account: Service account address when using a private key for earth engine authentication.
        use_personal_account: A flag to authenticate earth engine using personal account. Default: False.
    """

    def __init__(self,
                 asset_location: str,
                 ee_asset: str,
                 ee_asset_type: str,
                 ee_qps: int,
                 ee_latency: float,
                 ee_max_concurrent: int,
                 force: bool,
                 private_key: str,
                 service_account: str,
                 use_personal_account: bool,
                 use_metrics: bool):
        """Sets up rate limit and initializes the earth engine."""
        super().__init__(ee_qps=ee_qps,
                         ee_latency=ee_latency,
                         ee_max_concurrent=ee_max_concurrent,
                         private_key=private_key,
                         service_account=service_account,
                         use_personal_account=use_personal_account,
                         use_metrics=use_metrics)
        self.asset_location = asset_location
        self.ee_asset = ee_asset
        self.ee_asset_type = ee_asset_type
        self.force_overwrite = force
        self.use_metrics = use_metrics

    @timeit('FilterFileTransform')
    def process(self, uri: str) -> t.Iterator[str]:
        """Yields uri if the asset does not already exist."""
        project_id = self.ee_asset.split('/')[1]
        self.check_setup(project_id)
        asset_name = get_ee_safe_name(uri)

        # Checks if the asset is already present in the GCS bucket or not.
        target_path = os.path.join(
            self.asset_location, f'{asset_name}{ASSET_TYPE_TO_EXTENSION_MAPPING[self.ee_asset_type]}')
        if not self.force_overwrite and FileSystems.exists(target_path):
            logger.info(f'Asset file {target_path} already exists in GCS bucket. Skipping...')
            return

        asset_id = os.path.join(self.ee_asset, asset_name)
        try:
            ee.data.getAsset(asset_id)
            logger.info(f'Asset {asset_id} already exists in EE. Skipping...')
        except ee.EEException:
            yield uri


@dataclasses.dataclass
class ConvertToAsset(beam.DoFn, KwargsFactoryMixin):
    """Writes asset after extracting input data and uploads it to GCS.

    Attributes:
        ee_asset_type: The type of asset to ingest in the earth engine. Default: IMAGE.
        asset_location: The bucket location at which asset files will be pushed.
        xarray_open_dataset_kwargs: A dictionary of kwargs to pass to xr.open_dataset().
        disable_grib_schema_normalization: A flag to turn grib schema normalization off; Default: on.
    """

    asset_location: str
    ee_asset_type: str = 'IMAGE'
    xarray_open_dataset_kwargs: t.Optional[t.Dict] = None
    disable_grib_schema_normalization: bool = False
    group_common_hypercubes: t.Optional[bool] = False
    band_names_dict: t.Optional[t.Dict] = None
    initialization_time_regex: t.Optional[str] = None
    forecast_time_regex: t.Optional[str] = None
    use_deflate: t.Optional[bool] = False
    use_metrics: t.Optional[bool] = False

    def add_to_queue(self, queue: Queue, item: t.Any):
        """Adds a new item to the queue.

        It will wait until the queue has a room to add a new item.
        """
        while queue.full():
            pass
        queue.put_nowait(item)

    def convert_to_asset(self, queue: Queue, uri: str):
        """Converts source data into EE asset (GeoTiff or CSV) and uploads it to the bucket."""
        child_logger = logging.getLogger(__name__)
        child_logger.info(f'Converting {uri!r} to COGs...')

        job_start_time = get_utc_timestamp()

        with open_dataset(uri,
                          self.xarray_open_dataset_kwargs,
                          self.disable_grib_schema_normalization,
                          initialization_time_regex=self.initialization_time_regex,
                          forecast_time_regex=self.forecast_time_regex,
                          group_common_hypercubes=self.group_common_hypercubes) as ds_list:
            if not isinstance(ds_list, list):
                ds_list = [ds_list]

            for ds in ds_list:
                attrs = ds.attrs
                data = list(ds.values())
                asset_name = get_ee_safe_name(uri)
                channel_names = [
                    self.band_names_dict.get(da.name, da.name) if self.band_names_dict
                    else da.name for da in data
                ]

                dtype, crs, transform = (attrs.pop(key) for key in ['dtype', 'crs', 'transform'])
                # Adding job_start_time to properites.
                attrs["job_start_time"] = job_start_time
                # Make attrs EE ingestable.
                attrs = make_attrs_ee_compatible(attrs)
                start_time, end_time = (attrs.get(key) for key in ('start_time', 'end_time'))

                if self.group_common_hypercubes:
                    level, height = (attrs.pop(key) for key in ['level', 'height'])
                    safe_level_name = get_ee_safe_name(level)
                    asset_name = f'{asset_name}_{safe_level_name}'

                compression = 'lzw'
                predictor = 'NO'
                if self.use_deflate:
                    compression = 'deflate'
                    # Depending on dtype select predictor value.
                    # Predictor is a method of storing only the difference from the
                    # previous value instead of the actual value.
                    predictor = 2 if np.issubdtype(dtype, np.integer) else 3

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
                                          compress=compression,
                                          predictor=predictor) as f:
                            for i, da in enumerate(data):
                                f.write(da, i+1)
                                # Making the channel name EE-safe before adding it as a band name.
                                f.set_band_description(i+1, get_ee_safe_name(channel_names[i]))
                                f.update_tags(i+1, band_name=channel_names[i])
                                f.update_tags(i+1, **da.attrs)

                            # Write attributes as tags in tiff.
                            f.update_tags(**attrs)

                        # Copy in-memory tiff to gcs.
                        target_path = os.path.join(self.asset_location, file_name)
                        with FileSystems().create(target_path) as dst:
                            shutil.copyfileobj(memfile, dst, WRITE_CHUNK_SIZE)
                            child_logger.info(f"Uploaded {uri!r}'s COG to {target_path}")

                # For feature collection ingestions.
                elif self.ee_asset_type == 'TABLE':
                    channel_names = []
                    file_name = f'{asset_name}.csv'

                    shape = math.prod(list(ds.dims.values()))
                    # Names of dimesions, coordinates and data variables.
                    dims = list(ds.dims)
                    coords = [c for c in list(ds.coords) if c not in dims]
                    vars = list(ds.data_vars)
                    header = dims + coords + vars

                    # Data of dimesions, coordinates and data variables.
                    dims_data = [ds[dim].data for dim in dims]
                    coords_data = [np.full((shape,), ds[coord].data) for coord in coords]
                    vars_data = [ds[var].data.flatten() for var in vars]
                    data = coords_data + vars_data

                    dims_shape = [len(ds[dim].data) for dim in dims]

                    def get_dims_data(index: int) -> t.List[t.Any]:
                        """Returns dimensions for the given flattened index."""
                        return [
                            dim[int(index/math.prod(dims_shape[i+1:])) % len(dim)] for (i, dim) in enumerate(dims_data)
                        ]

                    # Copy CSV to gcs.
                    target_path = os.path.join(self.asset_location, file_name)
                    with tempfile.NamedTemporaryFile() as temp:
                        with open(temp.name, 'w', newline='') as f:
                            writer = csv.writer(f)
                            writer.writerows([header])
                            # Write rows in batches.
                            for i in range(0, shape, ROWS_PER_WRITE):
                                writer.writerows(
                                    [get_dims_data(i) + list(row) for row in zip(
                                        *[d[i:i + ROWS_PER_WRITE] for d in data]
                                    )]
                                )

                        upload(temp.name, target_path)

                asset_data = AssetData(
                    name=asset_name,
                    target_path=target_path,
                    channel_names=channel_names,
                    start_time=start_time,
                    end_time=end_time,
                    properties=attrs
                )

                self.add_to_queue(queue, asset_data)
            self.add_to_queue(queue, None)  # Indicates end of the subprocess.

    @timeit('ConvertToAsset')
    def process(self, uri: str) -> t.Iterator[AssetData]:
        """Opens grib files and yields AssetData.

        We observed that the convert-to-cog process increases memory usage over time because xarray (v2022.11.0) is not
        releasing memory as expected while opening any dataset. So we will perform the convert-to-asset process in an
        isolated process so that the memory consumed while processing will be cleared after the process is killed.

        The process puts the asset data into the queue which the main process will consume. Queue buffer size is limited
        so the process will be able to put another item in a queue only after the main process has consumed the queue
        item, that way it makes sure that no queue item is dropped due to queue buffer size.
        """
        queue = Queue(maxsize=1)
        process = Process(target=self.convert_to_asset, args=(queue, uri))
        process.start()

        while True:
            if not queue.empty():
                asset_data = queue.get_nowait()

                # Not needed now but keeping this check for backwards compatibility.
                if asset_data is None:
                    break
                yield asset_data

            # When the convert-to-asset process terminates unexpectedly...
            if not process.is_alive():
                logger.warning(f'Failed to convert {uri!r} to asset!')
                break

        process.terminate()


class IngestIntoEETransform(SetupEarthEngine, KwargsFactoryMixin):
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
                 use_personal_account: bool,
                 ingest_as_virtual_asset: bool,
                 use_metrics: bool):
        """Sets up rate limit."""
        super().__init__(ee_qps=ee_qps,
                         ee_latency=ee_latency,
                         ee_max_concurrent=ee_max_concurrent,
                         private_key=private_key,
                         service_account=service_account,
                         use_personal_account=use_personal_account,
                         use_metrics=use_metrics)
        self.ee_asset = ee_asset
        self.ee_asset_type = ee_asset_type
        self.ingest_as_virtual_asset = ingest_as_virtual_asset
        self.use_metrics = use_metrics

    def get_project_id(self) -> str:
        return self.ee_asset.split('/')[1]

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
    def start_ingestion(self, asset_data: AssetData) -> t.Optional[str]:
        """Creates COG-backed asset in earth engine. Returns the asset id."""
        project_id = self.get_project_id()
        self.check_setup(project_id)
        asset_name = os.path.join(self.ee_asset, asset_data.name)
        asset_data.properties['ingestion_time'] = get_utc_timestamp()

        try:
            logger.info(f"Uploading asset {asset_data.target_path} to Asset ID '{asset_name}'.")

            if self.ee_asset_type == 'IMAGE':  # Ingest an image.

                creds = get_creds(self.use_personal_account, self.service_account, self.private_key)
                session = AuthorizedSession(creds)

                image_manifest = {
                    'name': asset_name,
                    'tilesets': [
                        {
                            'id': '0',
                            'sources': [{'uris': [asset_data.target_path]}]
                        }
                    ],
                    'startTime': asset_data.start_time,
                    'endTime': asset_data.end_time,
                    'properties': asset_data.properties
                }

                headers = {
                    'Content-Type': 'application/json',
                    'x-goog-user-project': project_id,
                }

                data = json.dumps({'imageManifest': image_manifest, 'overwrite': True})

                if self.ingest_as_virtual_asset:  # as a virtual image.
                    # Makes an api call to register the virtual asset.
                    url = (
                        f'https://earthengine-highvolume.googleapis.com/v1/projects/{project_id}/'
                        f'image:import?overwrite=true&mode=VIRTUAL'
                    )
                else:  # as a COG based image.
                    url = (
                        f'https://earthengine-highvolume.googleapis.com/v1alpha/projects/{project_id}/'
                        f'image:importExternal'
                    )
                # Send API request
                response = session.post(url=url, data=data, headers=headers)
                logger.info(f"EE Asset ingestion response for {asset_name}: {response.text}")

                if response.status_code != 200:
                    logger.info(f"Failed to ingest asset '{asset_name}' in Earth Engine: {response.text}")
                    raise ee.EEException(response.text)
                if self.ingest_as_virtual_asset:
                    response_json = response.json()
                    ingestion_state = (
                        response_json
                        .get("metadata", {})
                        .get("state", "STATE_UNSPECIFIED")
                        .upper()
                    )
                    if ingestion_state != "SUCCEEDED":
                        raise ee.EEException(response.text)
                return asset_name
            elif self.ee_asset_type == 'TABLE':  # ingest a feature collection.
                self.wait_for_task_queue()
                task_id = ee.data.newTaskId(1)[0]
                ee.data.startTableIngestion(task_id, {
                    'name': asset_name,
                    'sources': [{
                        'uris': [asset_data.target_path]
                    }],
                    'startTime': asset_data.start_time,
                    'endTime': asset_data.end_time,
                    'properties': asset_data.properties
                })
                return asset_name
        except ee.EEException as e:
            if "Could not parse a valid CRS from the first overview of the GeoTIFF" in repr(e):
                logger.error(f"Failed to create asset '{asset_name}' in earth engine: {e}. Moving on...")
                return ""

            if (
                "whose type does not match the type of the same property of existing "
                "assets in the same collection"
                in repr(e)
            ):
                logger.error(f"Failed to ingest asset '{asset_name}' due to property mismatch: {e} Moving on...")
                return ""

            if "The metadata of the TIFF could not be read in the first 10000000 bytes." in repr(e):
                logger.error(f"Faild to ingest asset '{asset_name}', check the tiff file: {e} Moving on...")
                return ""

            logger.error(f"Failed to create asset '{asset_name}' in earth engine: {e}")
            # We do have logic for skipping the already created assets in FilterFilesTransform but
            # somehow we are observing that streaming pipeline reports "Cannot overwrite ..." error
            # so this will act as a quick fix for this issue.
            if f"Cannot overwrite asset '{asset_name}'" in repr(e):
                ee.data.deleteAsset(asset_name)
            raise

    @timeit('IngestIntoEE')
    def process(self, asset_data: AssetData) -> t.Iterator[t.Tuple[str, float]]:
        """Uploads an asset into the earth engine."""
        asset_name = self.start_ingestion(asset_data)
        if asset_name:
            metric.Metrics.counter('Success', 'IngestIntoEE').inc()
            asset_start_time = asset_data.start_time
            yield asset_name, asset_start_time
