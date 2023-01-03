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
import itertools
import logging
import socket
import subprocess
import sys
import typing as t

from apache_beam.utils import retry

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


def copy(src: str, dst: str) -> None:
    """Copy data via `gsutil cp`."""
    try:
        subprocess.run(['gsutil', 'cp', src, dst], check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        logger.error(f'Failed to copy file {src!r} to {dst!r} due to {e.stderr.decode("utf-8")}')
        raise
