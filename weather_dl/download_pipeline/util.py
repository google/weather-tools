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
import socket
import sys

from apache_beam.utils import retry


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
