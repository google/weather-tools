"""Download destinations, or `Store`s."""

import abc
import io
import os
import tempfile
import typing as t


class Store(abc.ABC):
    """A interface to represent where downloads are stored.

     Default implementation is Google Cloud Storage.
     """

    @abc.abstractmethod
    def open(self, filename: str, mode: str = 'r') -> t.IO:
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


class TempFileStore(Store):
    """Store data into temporary files."""

    def __init__(self, directory: t.Optional[str] = None) -> None:
        """Optionally specify the directory that contains all temporary files."""
        self.dir = directory
        if self.dir and not os.path.exists(self.dir):
            os.makedirs(self.dir)

    def open(self, filename: str, mode: str = 'r') -> t.IO:
        return tempfile.TemporaryFile(mode, dir=self.dir)
