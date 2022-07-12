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
import datetime
import itertools
import logging
import operator
import typing as t
from functools import reduce
import pandas as pd
import sys
import signal
import tempfile
import traceback
import uuid
import json
from functools import partial
from urllib.parse import urlparse

import numpy as np
import xarray as xr
from xarray.core.utils import ensure_us_time_resolution
from .sinks import DEFAULT_COORD_KEYS

from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound
from google.api_core.exceptions import BadRequest

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CANARY_BUCKET_NAME = 'anthromet_canary_bucket'
CANARY_RECORD = {'foo': 'bar'}
CANARY_RECORD_FILE_NAME = 'canary_record.json'
CANARY_OUTPUT_TABLE_SUFFIX = '_anthromet_canary_table'
CANARY_TABLE_SCHEMA = [bigquery.SchemaField('name', 'STRING', mode='NULLABLE')]


def to_json_serializable_type(value: t.Any) -> t.Any:
    """Returns the value with a type serializable to JSON"""
    # Note: The order of processing is significant.
    logger.debug('Serializing to JSON')

    if pd.isna(value) or value is None:
        return None
    elif np.issubdtype(type(value), np.floating):
        return float(value)
    elif type(value) == np.ndarray:
        # Will return a scaler if array is of size 1, else will return a list.
        return value.tolist()
    elif type(value) == datetime.datetime or type(value) == str or type(value) == np.datetime64:
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
    elif type(value) == np.timedelta64:
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


def _prod(xs: t.Iterable[int]) -> int:
    return reduce(operator.mul, xs, 1)


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
    total_coords = _prod(ds.coords.dims.values())
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
