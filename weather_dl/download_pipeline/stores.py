# Copyright 2021 Google LLC
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

"""Download destinations, or `Store`s."""

import abc
import io
import os
import tempfile
import typing as t

from apache_beam.io.filesystems import FileSystems


class Store(abc.ABC):
    """A interface to represent where downloads are stored.

     Default implementation is Google Cloud Storage.
     """

    @abc.abstractmethod
    def open(self, filename: str, mode: str = 'r') -> t.IO:
        pass

    @abc.abstractmethod
    def exists(self, filename: str) -> bool:
        pass


class InMemoryStore(Store):
    """Store file data in memory."""

    def __init__(self):
        self.store = {}

    def open(self, filename: str, mode: str = 'r') -> t.IO:
        if 'b' in mode:
            file = io.BytesIO()
        else:
            file = io.StringIO()
        self.store[filename] = file
        return file

    def exists(self, filename: str) -> bool:
        return filename in self.store


class TempFileStore(Store):
    """Store data into temporary files."""

    def __init__(self, directory: t.Optional[str] = None) -> None:
        """Optionally specify the directory that contains all temporary files."""
        self.dir = directory
        if self.dir and not os.path.exists(self.dir):
            os.makedirs(self.dir)

    def open(self, filename: str, mode: str = 'r') -> t.IO:
        return tempfile.TemporaryFile(mode, dir=self.dir)

    def exists(self, filename: str) -> bool:
        return os.path.exists(filename)


class LocalFileStore(Store):
    """Store data into local files."""

    def __init__(self, directory: t.Optional[str] = None) -> None:
        """Optionally specify the directory that contains all downloaded files."""
        self.dir = directory
        if self.dir and not os.path.exists(self.dir):
            os.makedirs(self.dir)

    def open(self, filename: str, mode: str = 'r') -> t.IO:
        return open('{}/{}'.format(self.dir, filename), mode)

    def exists(self, filename: str) -> bool:
        return os.path.exists('{}/{}'.format(self.dir, filename))


class FSStore(Store):
    """Store data into any store supported by Apache Beam's FileSystems."""

    def open(self, filename: str, mode: str = 'r') -> t.IO:
        if 'r' in mode and 'w' not in mode:
            return FileSystems().open(filename)

        if 'w' in mode and 'r' not in mode:
            return FileSystems().create(filename)

        raise ValueError("invalid mode: mode must have either 'r' or 'w', but not both.")

    def exists(self, filename: str) -> bool:
        return FileSystems().exists(filename)
