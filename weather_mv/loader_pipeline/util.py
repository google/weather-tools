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
import abc
import datetime
import inspect
import itertools
import json
import logging
import math
import re
import signal
import sys
import tempfile
import time
import traceback
import typing as t
import uuid
from functools import partial
from urllib.parse import urlparse

import apache_beam as beam
import numpy as np
import pandas as pd
import xarray as xr
from google.api_core.exceptions import BadRequest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage
from xarray.core.utils import ensure_us_time_resolution

from .sinks import DEFAULT_COORD_KEYS

logger = logging.getLogger(__name__)

CANARY_BUCKET_NAME = 'anthromet_canary_bucket'
CANARY_RECORD = {'foo': 'bar'}
CANARY_RECORD_FILE_NAME = 'canary_record.json'
CANARY_OUTPUT_TABLE_SUFFIX = '_anthromet_canary_table'
CANARY_TABLE_SCHEMA = [bigquery.SchemaField('name', 'STRING', mode='NULLABLE')]


def make_attrs_ee_compatible(attrs: t.Dict) -> t.Dict:
    """Scans EEE asset attributes and makes them EE compatible.

    EE asset attribute names must contain only the following characters: A..Z,
    a..z, 0..9 or '_'. Maximum length is 110 characters. Attribute values must be
    string or number.

    If an attribute name is more than 110 characters, it will consider the first
    110 characters as the attribute name.
    """
    new_attrs = {}

    for k, v in attrs.items():
        if len(k) > 110:  # Truncate attribute name with > 110 characters.
            k = k[:110]
        # Replace unaccepted characters with underscores.
        k = re.sub(r'[^a-zA-Z0-9-_]+', r'_', k)

        if type(v) not in [int, float]:
            v = str(v)
            if len(v) > 1024:
                v = f'{v[:1021]}...'  # Since 1 char = 1 byte.

        v = to_json_serializable_type(v)
        new_attrs[k] = v

    return new_attrs


# TODO(#245): Group with common utilities (duplicated)
def to_json_serializable_type(value: t.Any) -> t.Any:
    """Returns the value with a type serializable to JSON"""
    # Note: The order of processing is significant.
    logger.debug('Serializing to JSON')

    # pd.isna() returns ndarray if input is not scalar therefore checking if value is scalar.
    if (np.isscalar(value) and pd.isna(value)) or value is None:
        return None
    elif np.issubdtype(type(value), np.floating):
        return float(value)
    elif isinstance(value, set):
        value = list(value)
        return np.where(pd.isna(value), None, value).tolist()
    elif isinstance(value, np.ndarray):
        # Will return a scaler if array is of size 1, else will return a list.
        # Replace all NaNs, NaTs with None.
        return np.where(pd.isna(value), None, value).tolist()
    elif isinstance(value, datetime.datetime) or isinstance(value, str) or isinstance(value, np.datetime64):
        # Assume strings are ISO format timestamps...
        try:
            value = datetime.datetime.fromisoformat(value)
        except ValueError:
            # ... if they are not, assume serialization is already correct.
            return value
        except TypeError:
            # ... maybe value is a numpy datetime ...
            try:
                value = ensure_us_time_resolution(value).astype(datetime.datetime)
            except AttributeError:
                # ... value is a datetime object, continue.
                pass

        # We use a string timestamp representation.
        if value.tzname():
            return value.isoformat()

        # We assume here that naive timestamps are in UTC timezone.
        return value.replace(tzinfo=datetime.timezone.utc).isoformat()
    elif isinstance(value, datetime.timedelta):
        return value.total_seconds()
    elif isinstance(value, np.timedelta64):
        # Return time delta in seconds.
        return float(value / np.timedelta64(1, 's'))
    # This check must happen after processing np.timedelta64 and np.datetime64.
    elif np.issubdtype(type(value), np.integer):
        return int(value)

    return value


def _check_for_coords_vars(ds_data_var: str, target_var: str) -> bool:
    """Checks if the dataset's data variable matches with the target variables (or coordinates)
    specified by the user."""
    return ds_data_var.endswith('_'+target_var) or ds_data_var.startswith(target_var+'_')


def _only_target_coordinate_vars(ds: xr.Dataset, data_vars: t.List[str]) -> t.List[str]:
    """If the user specifies target fields in the dataset, get all the matching coords & data vars."""

    # If the dataset is not the merged dataset (created for normalizing grib's schema),
    # return the target fields specified by the user as it is.
    if not ds.attrs['is_normalized']:
        return data_vars

    keep_coords_vars = []

    for dv in data_vars:
        keep_coords_vars.extend([v for v in ds.data_vars if _check_for_coords_vars(v, dv)])
        keep_coords_vars.extend([v for v in DEFAULT_COORD_KEYS if v in dv])

    return keep_coords_vars


def _only_target_vars(ds: xr.Dataset, data_vars: t.Optional[t.List[str]] = None) -> xr.Dataset:
    """If the user specifies target fields in the dataset, create a schema only from those fields."""

    # If there are no restrictions on data vars, include the whole dataset.
    if not data_vars:
        logger.info(f'target data_vars empty; using whole dataset; size: {ds.nbytes}')
        return ds

    if not ds.attrs['is_normalized']:
        assert all([dv in ds.data_vars or dv in ds.coords for dv in data_vars]), 'Target variable must be in original '\
                                                                                'dataset. '

        dropped_ds = ds.drop_vars([v for v in ds.data_vars if v not in data_vars])
    else:
        drop_vars = []
        check_target_variable = []
        for dv in data_vars:
            searched_data_vars = [_check_for_coords_vars(v, dv) for v in ds.data_vars]
            searched_coords = [] if dv not in DEFAULT_COORD_KEYS else [dv in ds.coords]
            check_target_variable.append(any(searched_data_vars) or any(searched_coords))

        assert all(check_target_variable), 'Target variable must be in original dataset.'

        for v in ds.data_vars:
            searched_data_vars = [_check_for_coords_vars(v, dv) for dv in data_vars]
            if not any(searched_data_vars):
                drop_vars.append(v)

        dropped_ds = ds.drop_vars(drop_vars)

    logger.info(f'target-only dataset size: {dropped_ds.nbytes}')

    return dropped_ds


def ichunked(iterable: t.Iterable, n: int) -> t.Iterator[t.Iterable]:
    """Yield evenly-sized chunks from an iterable."""
    input_ = iter(iterable)
    try:
        while True:
            it = itertools.islice(input_, n)
            # peek to check if 'it' has next item.
            first = next(it)
            yield itertools.chain([first], it)
    except StopIteration:
        pass


def get_coordinates(ds: xr.Dataset, uri: str = '') -> t.Iterator[t.Dict]:
    """Generates normalized coordinate dictionaries that can be used to index Datasets with `.loc[]`."""
    # Creates flattened iterator of all coordinate positions in the Dataset.
    #
    # Coordinates have been pre-processed to remove NaNs and to format datetime objects
    # to ISO format strings.
    #
    # Example: (-108.0, 49.0, '2018-01-02T22:00:00+00:00')
    coords = itertools.product(
        *(
            (
                to_json_serializable_type(v)
                for v in ensure_us_time_resolution(ds[c].variable.values).tolist()
            )
            for c in ds.coords.indexes
        )
    )
    # Give dictionary keys to a coordinate index.
    #
    # Example:
    #   {'longitude': -108.0, 'latitude': 49.0, 'time': '2018-01-02T23:00:00+00:00'}
    idx = 0
    total_coords = math.prod(ds.coords.dims.values())
    for idx, it in enumerate(coords):
        if idx % 1000 == 0:
            logger.info(f'Processed {idx // 1000}k / {(total_coords / 1000):.2f}k coordinates for {uri!r}...')
        yield dict(zip(ds.coords.indexes, it))

    logger.info(f'Finished processing all {(idx / 1000):.2f}k coordinates.')


def _cleanup_bigquery(bigquery_client: bigquery.Client,
                      canary_output_table: str,
                      sig: t.Optional[t.Any] = None,
                      frame: t.Optional[t.Any] = None) -> None:
    """Deletes the bigquery table."""
    if bigquery_client:
        bigquery_client.delete_table(canary_output_table, not_found_ok=True)
    if sig:
        traceback.print_stack(frame)
        sys.exit(0)


def _cleanup_bucket(storage_client: storage.Client,
                    canary_bucket_name: str,
                    sig: t.Optional[t.Any] = None,
                    frame: t.Optional[t.Any] = None) -> None:
    """Deletes the bucket."""
    try:
        storage_client.get_bucket(canary_bucket_name).delete(force=True)
    except NotFound:
        pass
    if sig:
        traceback.print_stack(frame)
        sys.exit(0)


def validate_region(output_table: t.Optional[str] = None,
                    temp_location: t.Optional[str] = None,
                    region: t.Optional[str] = None) -> None:
    """Validates non-compatible regions scenarios by performing sanity check."""
    if not region and not temp_location:
        raise ValueError('Invalid GCS location: None.')

    bucket_region = region
    storage_client = storage.Client()
    canary_bucket_name = CANARY_BUCKET_NAME + str(uuid.uuid4())

    # Doing cleanup if operation get cut off midway.
    # TODO : Should we handle some other signals ?
    do_bucket_cleanup = partial(_cleanup_bucket, storage_client, canary_bucket_name)
    original_sigint_handler = signal.getsignal(signal.SIGINT)
    original_sigtstp_handler = signal.getsignal(signal.SIGTSTP)
    signal.signal(signal.SIGINT, do_bucket_cleanup)
    signal.signal(signal.SIGTSTP, do_bucket_cleanup)

    if output_table:
        table_region = None
        bigquery_client = bigquery.Client()
        canary_output_table = output_table + CANARY_OUTPUT_TABLE_SUFFIX + str(uuid.uuid4())
        do_bigquery_cleanup = partial(_cleanup_bigquery, bigquery_client, canary_output_table)
        signal.signal(signal.SIGINT, do_bigquery_cleanup)
        signal.signal(signal.SIGTSTP, do_bigquery_cleanup)

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

        if output_table:
            table = bigquery.Table(canary_output_table, schema=CANARY_TABLE_SCHEMA)
            table = bigquery_client.create_table(table, exists_ok=True)
            table_region = table.location

            load_job = bigquery_client.load_table_from_uri(
                f'gs://{canary_bucket_name}/{CANARY_RECORD_FILE_NAME}',
                canary_output_table,
            )
            load_job.result()
    except BadRequest:
        if output_table:
            raise RuntimeError(f'Can\'t migrate from source: {bucket_region} to destination: {table_region}')
        raise RuntimeError(f'Can\'t upload to destination: {bucket_region}')
    finally:
        _cleanup_bucket(storage_client, canary_bucket_name)
        if output_table:
            _cleanup_bigquery(bigquery_client, canary_output_table)
        signal.signal(signal.SIGINT, original_sigint_handler)
        signal.signal(signal.SIGINT, original_sigtstp_handler)


def _shard(elem, num_shards: int):
    return (np.random.randint(0, num_shards), elem)


class RateLimit(beam.PTransform, abc.ABC):
    """PTransform to extend to apply a global rate limit to an operation.

    The input PCollection and be of any type and the output will be whatever is
    returned by the `process` method.
    """

    def __init__(self,
                 global_rate_limit_qps: int,
                 latency_per_request: float,
                 max_concurrent_requests: int):
        """Creates a RateLimit object.

        global_rate_limit_qps and latency_per_request are used to determine how the
        data should be sharded via:
        global_rate_limit_qps * latency_per_request.total_seconds()

        For example, global_rate_limit_qps = 500 and latency_per_request=.5 seconds.
        Then the data will be sharded into 500*.5=250 groups.  Each group can be
        processed in parallel and will call the 'process' function at most once
        every latency_per_request.

        It is important to note that the max QPS may not be reach based on how many
        workers are scheduled.

        Args:
            global_rate_limit_qps: QPS to rate limit requests across all workers to.
            latency_per_request: The expected latency per request.
            max_concurrent_requests: Maximum allowed concurrent api requests to EE.
        """

        self._rate_limit = global_rate_limit_qps
        self._latency_per_request = datetime.timedelta(seconds=latency_per_request)
        self._num_shards = max(1, min(int(self._rate_limit * self._latency_per_request.total_seconds()),
                                      max_concurrent_requests))

    @abc.abstractmethod
    def process(self, elem: t.Any):
        """Process is the operation that will be rate limited.

        Results will be yielded each time time the process method is called.

        Args:
            elem: The individual element to process.

        Returns:
            Output can be anything, output will be the output of the RateLimit
            PTransform.
        """
        pass

    def expand(self, pcol: beam.PCollection):
        return (pcol
                | beam.Map(_shard, self._num_shards)
                | beam.GroupByKey()
                | beam.ParDo(
                    _RateLimitDoFn(self.process, self._latency_per_request)))


class _RateLimitDoFn(beam.DoFn):
    """DoFn that ratelimits calls to rate_limit_fn."""

    def __init__(self, rate_limit_fn: t.Callable, wait_time: datetime.timedelta):
        self._rate_limit_fn = rate_limit_fn
        self._wait_time = wait_time
        self._is_generator = inspect.isgeneratorfunction(self._rate_limit_fn)  # type: ignore

    def process(self, keyed_elem: t.Tuple[t.Any, t.Iterable[t.Any]]):
        shard, elems = keyed_elem
        logger.info(f'processing shard: {shard}')

        start_time = datetime.datetime.now()
        end_time = None
        for elem in elems:
            if end_time is not None and (end_time - start_time) < self._wait_time:
                logger.info(f'previous operation took: {(end_time - start_time).total_seconds()}')
                wait_time = (self._wait_time - (end_time - start_time))
                logger.info(f'wating: {wait_time.total_seconds()}')
                time.sleep(wait_time.total_seconds())
            start_time = datetime.datetime.now()
            if self._is_generator:
                yield from self._rate_limit_fn(elem)
            else:
                yield self._rate_limit_fn(elem)
            end_time = datetime.datetime.now()
