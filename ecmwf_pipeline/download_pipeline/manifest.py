"""Client interface for connecting to a manifest."""

import abc
import collections
import time
import typing as t
import logging
import json

from apache_beam.io.gcp import gcsio

Location = t.NewType('Location', str)


class ManifestException(Exception):
    pass


class DownloadStatus(t.NamedTuple):
    """Data recorded in `Manifest`s reflecting the status of a download."""

    """Copy of selection section of the configuration."""
    selection: t.Dict

    """Location of the downloaded data."""
    location: str

    """Download status: 'scheduled', 'in-progress', 'success', or 'failure'."""
    status: str

    """Cause of error"""
    error: t.Optional[str]

    """Identifier for the user running the download."""
    user: str

    """Time in milliseconds since epoch."""
    download_finished_time: t.Optional[int]

    """Duration in milliseconds."""
    download_duration: t.Optional[int]


class ManifestTransaction:
    """A context manager for manifests that marks in-progress, success, or failure download statuses."""

    def __init__(self, update: t.Callable[[DownloadStatus], None], selection: t.Dict, location: str, user: str) -> None:
        self.update = update
        self.start = 0
        self.status = DownloadStatus(
            selection=selection,
            location=location,
            user=user,
            status='in-progress',
            error=None,
            download_finished_time=None,
            download_duration=None
        )

    def __enter__(self) -> None:
        self.start = time.time()
        self.update(self.status)

    def __exit__(self, exc_type, exc_inst, exc_tb) -> bool:
        end = time.time()
        if exc_type is None:
            status = 'success'
            error = None
        else:
            status = 'failure'
            error = str(exc_inst)

        self.status = DownloadStatus(
            selection=self.status.selection,
            location=self.status.location,
            user=self.status.user,
            status=status,
            error=error,
            download_finished_time=int(end),
            download_duration=int(end - self.start)
        )
        self.update(self.status)
        # A truthy return value from this function will not propagate exceptions
        # to the caller. https://docs.python.org/3/reference/datamodel.html#object.__exit__
        return status == 'failure'


class Manifest(abc.ABC):
    """Manifest client interface."""

    def __init__(self, location: Location) -> None:
        """Initialize the manifest."""
        self.location = location

    def schedule(self, selection: t.Dict, location: str, user: str) -> None:
        """Indicate that a job has been scheduled for download.

        'scheduled' jobs occur before 'in-progress', 'success' or 'finished'.
        """
        self._update(
            DownloadStatus(
                selection=selection,
                location=location,
                user=user,
                status='scheduled',
                error=None,
                download_finished_time=None,
                download_duration=None
            )
        )

    def transact(self, selection: t.Dict, location: str, user: str) -> ManifestTransaction:
        """Create a download transaction."""
        return ManifestTransaction(self._update, selection, location, user)

    @abc.abstractmethod
    def _update(self, download_status: DownloadStatus) -> None:
        pass


class GCSManifest(Manifest):
    """Writes a JSON representation of the manifest to GCS.

    This is an append-only implementation, the latest value in the manifest
    represents the current state of a download.
    """

    def __init__(self, location: Location) -> None:
        super().__init__(location)
        self.logger = logging.getLogger(GCSManifest.__name__)

    def _update(self, download_status: DownloadStatus) -> None:
        """Writes the JSON data to a manifest."""
        with gcsio.GcsIO().open(self.location, 'a') as gcs_file:
            json.dump(download_status._asdict(), gcs_file)
            self.logger.info('Manifest written to.')
            self.logger.debug(download_status)


class MockManifest(Manifest):
    """In-memory mock manifest."""

    def __init__(self, location: Location) -> None:
        super().__init__(location)
        self.records = {}
        self.logger = logging.getLogger(MockManifest.__name__)

    def _update(self, download_status: DownloadStatus) -> None:
        self.records.update({download_status.location: download_status})
        self.logger.info('Manifest updated.')
        self.logger.debug(download_status)


class NoOpManifest(Manifest):
    def _update(self, download_status: DownloadStatus) -> None:
        pass


"""Exposed manifest implementations.

Users can choose their preferred manifest implementation by via the protocol of the Manifest Location.
The protocol corresponds to the keys of this ordered dictionary.

If no key is found, the `NoOpManifest` option will be chosen. See `parsers:parse_manifest_location`.
"""
MANIFESTS = collections.OrderedDict(
    gs=GCSManifest,
)
