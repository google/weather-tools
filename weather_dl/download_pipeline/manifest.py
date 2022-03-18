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
"""Client interface for connecting to a manifest."""

import abc
import collections
import dataclasses
import json
import logging
import os
import threading
import time
import traceback
import typing as t
from urllib.parse import urlparse, parse_qsl

import firebase_admin
from apache_beam.io.gcp import gcsio
from firebase_admin import firestore
from google.cloud.firestore_v1 import DocumentReference
from google.cloud.firestore_v1.types import WriteResult

"""An implementation-dependent Manifest URI."""
Location = t.NewType('Location', str)

logger = logging.getLogger(__name__)


class ManifestException(Exception):
    """Errors that occur in Manifest Clients."""
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

    """Time in milliseconds since epoch of when download was scheduled."""
    download_scheduled_time: t.Optional[int]

    """Time in milliseconds since epoch."""
    download_finished_time: t.Optional[int]

    """Duration in milliseconds."""
    download_duration: t.Optional[int]


@dataclasses.dataclass
class Manifest(abc.ABC):
    """Abstract manifest of download statuses.

    Update download statuses to some storage medium.

    This class lets one indicate that a download is `scheduled` or in a transaction process.
    In the event of a transaction, a download will be updated with an `in-progress`, `success`
    or `failure` status (with accompanying metadata).

    Example:
        ```
        my_manifest = parse_manifest_location(Location('fs://some-firestore-collection'))

        # Schedule data for download
        my_manifest.schedule({'some': 'metadata'}, 'path/to/downloaded/file', 'my-username')

        # ...

        # Initiate a transaction – it will record that the download is `in-progess`
        with my_manifest.transact({'some': 'metadata'}, 'path/to/downloaded/file', 'my-username') as tx:
            # download logic here
            pass

            # ...

            # on error, will record the download as a `failure` before propagating the error.  By default, it will
            # record download as a `success`.
        ```

    Attributes:
        location: An implementation-specific manifest URI.
        status: The current `DownloadStatus` of the Manifest.
    """

    location: Location
    status: t.Optional[DownloadStatus] = None

    def __post_init__(self):
        """Initialize the manifest."""
        self.start = 0
        self.scheduled_times = {}

    def schedule(self, selection: t.Dict, location: str, user: str) -> None:
        """Indicate that a job has been scheduled for download.

        'scheduled' jobs occur before 'in-progress', 'success' or 'finished'.
        """
        scheduled_time = int(time.time())
        self.scheduled_times[location] = scheduled_time
        self._update(
            DownloadStatus(
                selection=selection,
                location=location,
                user=user,
                status='scheduled',
                error=None,
                download_scheduled_time=scheduled_time,
                download_finished_time=None,
                download_duration=None
            )
        )

    def _set_for_transaction(self, selection: t.Dict, location: str, user: str) -> None:
        """Reset Manifest state in preparation for a new transaction."""
        self.start = 0
        self.status = DownloadStatus(
            selection=selection,
            location=location,
            user=user,
            status='in-progress',
            error=None,
            download_scheduled_time=self.scheduled_times.pop(location, None),
            download_finished_time=None,
            download_duration=None
        )

    def __enter__(self) -> None:
        """Record 'in-progress' status of a transaction."""
        self.start = time.time()
        self._update(self.status)

    def __exit__(self, exc_type, exc_inst, exc_tb) -> None:
        """Record end status of a transaction as either 'success' or 'failure'."""
        end = time.time()
        if exc_type is None:
            status = 'success'
            error = None
        else:
            status = 'failure'
            # For explanation, see https://docs.python.org/3/library/traceback.html#traceback.format_exception
            error = '\n'.join(traceback.format_exception(exc_type, exc_inst, exc_tb))

        self.status = DownloadStatus(
            selection=self.status.selection,
            location=self.status.location,
            user=self.status.user,
            status=status,
            error=error,
            download_scheduled_time=self.status.download_scheduled_time,
            download_finished_time=int(end),
            download_duration=int(end - self.start)
        )
        self._update(self.status)

    def transact(self, selection: t.Dict, location: str, user: str) -> 'Manifest':
        """Create a download transaction."""
        self._set_for_transaction(selection, location, user)
        return self

    @abc.abstractmethod
    def _update(self, download_status: DownloadStatus) -> None:
        pass


class GCSManifest(Manifest):
    """Writes a JSON representation of the manifest to GCS.

    This is an append-only implementation, the latest value in the manifest
    represents the current state of a download.
    """

    # Ensure no race conditions occurs on appends to objects in GCS
    # (i.e. JSON manifests).
    _lock = threading.Lock()

    def _update(self, download_status: DownloadStatus) -> None:
        """Writes the JSON data to a manifest."""
        with GCSManifest._lock:
            with gcsio.GcsIO().open(self.location, 'a') as gcs_file:
                json.dump(download_status._asdict(), gcs_file)
        logger.debug('Manifest written to.')
        logger.debug(download_status)


class LocalManifest(Manifest):
    """Writes a JSON representation of the manifest to local file."""

    _lock = threading.Lock()

    def __init__(self, location: Location) -> None:
        super().__init__(Location('{}{}manifest.json'.format(location, os.sep)))
        if location and not os.path.exists(location):
            os.makedirs(location)

        # If the file is empty, it should start out as an empty JSON object.
        if not os.path.exists(self.location) or os.path.getsize(self.location) == 0:
            with open(self.location, 'w') as file:
                json.dump({}, file)

    def _update(self, download_status: DownloadStatus) -> None:
        """Writes the JSON data to a manifest."""
        assert os.path.exists(self.location), f'{self.location} must exist!'
        with LocalManifest._lock:
            with open(self.location, 'r') as file:
                manifest = json.load(file)

            status = download_status._asdict()
            manifest[status['location']] = status

            with open(self.location, 'w') as file:
                json.dump(manifest, file)
                logger.debug('Manifest written to.')
                logger.debug(download_status)


class MockManifest(Manifest):
    """In-memory mock manifest."""

    def __init__(self, location: Location) -> None:
        super().__init__(location)
        self.records = {}

    def _update(self, download_status: DownloadStatus) -> None:
        self.records.update({download_status.location: download_status})
        logger.debug('Manifest updated.')
        logger.debug(download_status)


class NoOpManifest(Manifest):
    """A manifest that performs no operations."""

    def _update(self, download_status: DownloadStatus) -> None:
        pass


def get_wait_interval(num_retries: int = 0) -> float:
    """Returns next wait interval in seconds, using an exponential backoff algorithm."""
    if 0 == num_retries:
        return 0
    return 2 ** num_retries


class FirestoreManifest(Manifest):
    """A Firestore Manifest.

    This Manifest implementation stores DownloadStatuses in a Firebase document store.

    The document hierarchy for the manifest is as follows:

      [downloader-manifest  <or manifest name, configurable from CLI>]
      ├── scheme (e.g. 'gs, 's3', etc.) {}
      │   └── [bucket root name]
      │       └── doc_id (a base64 encoding of the path) { 'selection': {...}, 'location': ..., 'user': ... }
      └── etc...

    Where `[<name>]` indicates a collection and `<name> {...}` indicates a document.
    """

    def _get_db(self) -> firestore.firestore.Client:
        """Acquire a firestore client, initializing the firebase app if necessary.

        Will attempt to get the db client five times. If it's still unsuccessful, a
        `ManifestException` will be raised.
        """
        db = None
        attempts = 0

        while db is None:
            try:
                db = firestore.client()
            except ValueError as e:
                # The above call will fail with a value error when the firebase app is not initialized.
                # Initialize the app here, and try again.
                firebase_admin.initialize_app(options=self.get_firestore_config())
                logger.info('Initialized Firebase App.')

                if attempts > 4:
                    raise ManifestException('Exceeded number of retries to get firestore client.') from e

            time.sleep(get_wait_interval(attempts))

            attempts += 1

        return db

    def _update(self, download_status: DownloadStatus) -> None:
        """Update or create a download status record."""
        logger.debug('Updating Firestore Manifest.')

        # Get info for the document path
        parsed_location = urlparse(download_status.location)
        scheme = parsed_location.scheme or 'local'
        doc_id = parsed_location.path[1:].replace('/', '--')

        # Update document with download status
        download_doc_ref = (
            self.root_document_for_store(scheme)
                .collection(parsed_location.netloc)
                .document(doc_id)
        )
        result: WriteResult = download_doc_ref.set(download_status._asdict())

        logger.debug(f'Firestore manifest updated. '
                     f'update_time={result.update_time}, '
                     f'filename={download_status.location}.')

    def root_document_for_store(self, store_scheme: str) -> DocumentReference:
        """Get the root manifest document given the user's config and current document's storage location."""
        # Get user-defined collection for manifest.
        root_collection = self.get_firestore_config().get('collection', 'downloader-manifest')
        return self._get_db().collection(root_collection).document(store_scheme)

    def get_firestore_config(self) -> t.Dict:
        """Parse firestore Location format: 'fs://<collection-name>?projectId=<project-id>'

        Users must specify a 'projectId' query parameter in the firestore location. If this argument
        isn't passed in, users must set the `GOOGLE_CLOUD_PROJECT` environment variable.

        Users may specify options to `firebase_admin.initialize_app()` via query arguments in the URL.
        For more information about what options are available, consult this documentation:
        https://firebase.google.com/docs/reference/admin/python/firebase_admin#initialize_app

            Note: each query key-value pair may only appear once. If there are duplicates, the last pair
            will be used.

        Optionally, users may configure these options via the `FIREBASE_CONFIG` environment variable,
        which is typically a path/to/a/file.json.

        Examples:
            >>> location = Location("fs://my-collection?projectId=my-project-id&storageBucket=foo")
            >>> FirestoreManifest(location).get_firestore_config()
            {'collection': 'my-collection', 'projectId': 'my-project-id', 'storageBucket': 'foo'}

        Raises:
            ValueError: If query parameters are malformed.
            AssertionError: If the 'projectId' query parameter is not set.
        """
        parsed = urlparse(self.location)
        query_params = {}
        if parsed.query:
            query_params = dict(parse_qsl(parsed.query, strict_parsing=True))
        return {'collection': parsed.netloc, **query_params}


"""Exposed manifest implementations.

Users can choose their preferred manifest implementation by via the protocol of the Manifest Location.
The protocol corresponds to the keys of this ordered dictionary.

If no protocol is specified, we assume the user wants to write to the local file system.
If no key is found, the `NoOpManifest` option will be chosen. See `parsers:parse_manifest_location`.
"""
MANIFESTS = collections.OrderedDict({
    'fs': FirestoreManifest,
    'gs': GCSManifest,
    '': LocalManifest,
})

if __name__ == '__main__':
    # Execute doc tests
    import doctest
    doctest.testmod()
