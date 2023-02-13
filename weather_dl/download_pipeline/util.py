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
import itertools
import logging
import os
import socket
import subprocess
import sys
import typing as t

import numpy as np
import pandas as pd
from google.cloud import storage
from apache_beam.utils import retry
from urllib.parse import ParseResult
from xarray.core.utils import ensure_us_time_resolution

logger = logging.getLogger(__name__)


def _retry_if_valid_input_but_server_or_socket_error_and_timeout_filter(exception) -> bool:
    if isinstance(exception, socket.timeout):
        return True
    if isinstance(exception, TimeoutError):
        return True
    return retry.retry_if_valid_input_but_server_error_and_timeout_filter(exception)


class _FakeClock:
    def sleep(self, value):
        pass


def retry_with_exponential_backoff(fun):
    """A retry decorator that doesn't apply during test time."""
    clock = retry.Clock()

    # Use a fake clock only during test time...
    if 'unittest' in sys.modules.keys():
        clock = _FakeClock()

    return retry.with_exponential_backoff(
        retry_filter=_retry_if_valid_input_but_server_or_socket_error_and_timeout_filter,
        clock=clock,
    )(fun)


# TODO(#245): Group with common utilities (duplicated)
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


# TODO(#245): Group with common utilities (duplicated)
def copy(src: str, dst: str) -> None:
    """Copy data via `gsutil cp`."""
    try:
        subprocess.run(['gsutil', 'cp', src, dst], check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        logger.error(f'Failed to copy file {src!r} to {dst!r} due to {e.stderr.decode("utf-8")}')
        raise


# TODO(#245): Group with common utilities (duplicated)
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


class FileSizeStrategy(abc.ABC):
    @abc.abstractmethod
    def get_file_size(self, path: t.Union[ParseResult, str]) -> float:
        raise NotImplementedError


class GCSBlobSizeStrategy(FileSizeStrategy):
    def get_file_size(self, path: ParseResult) -> float:
        bucket_name = path.netloc
        object_name = path.path[1:]
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.get_blob(object_name)
        return blob.size / (10**9) if blob else 0


class LocalSystemFileSizeStrategy(FileSizeStrategy):
    def get_file_size(self, path: str) -> float:
        return os.stat(path).st_size / (1024 ** 3) if os.path.exists(path) else 0
