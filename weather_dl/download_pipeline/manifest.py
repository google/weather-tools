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
import datetime
from enum import Enum
import json
import logging
import os
import pandas as pd
import threading
import traceback
import typing as t

from .util import to_json_serializable_type
from google.cloud import bigquery, storage
from apache_beam.io.gcp import gcsio
from urllib.parse import urlparse

"""An implementation-dependent Manifest URI."""
Location = t.NewType('Location', str)

logger = logging.getLogger(__name__)


class ManifestException(Exception):
    """Errors that occur in Manifest Clients."""
    pass


class Stage(Enum):
    """A request can be either in one of the following stages at a time:

    fetch : This represents request is currently in fetch stage i.e. request placed on the client's server
        & waiting for some result before starting download (eg. MARS client).
    download : This represents request is currently in download stage i.e. data is being downloading from client's
        server to the worker's local file system.
    upload : This represents request is currently in upload stage i.e. data is getting uploaded from worker's local
        file system to target location (GCS path).
    retrieve : In case of clients where there is no proper separation of fetch & download stages (eg. CDS client),
        request will be in the retrieve stage i.e. fetch + download.
    None :  This represents a request that is just 'scheduled' for processing.
    """
    RETRIEVE = 'retrieve'
    FETCH = 'fetch'
    DOWNLOAD = 'download'
    UPLOAD = 'upload'
    NONE = None


class Status(Enum):
    """Depicts the request's state status:

    scheduled : A request partition is created & scheduled for processing.
        Note: Its corresponding state can be None only.
    in-progress : This represents the request state is currently in-progress (i.e. running).
        The next status would be "success" or "failure".
    success : This represents the request state execution completed successfully without any error.
    failure : This represents the request state execution failed.
    """
    SCHEDULED = 'scheduled'
    IN_PROGRESS = 'in-progress'
    SUCCESS = 'success'
    FAILURE = 'failure'


@dataclasses.dataclass
class DownloadStatus():
    """Data recorded in `Manifest`s reflecting the status of a download."""

    """Copy of selection section of the configuration."""
    selection: t.Dict = dataclasses.field(default_factory=dict)

    """Location of the downloaded data."""
    location: str = ""

    """Current stage of request : 'fetch', 'download', 'retrieve', 'upload' or None."""
    stage: t.Optional[Stage] = Stage.NONE

    """Download status: 'scheduled', 'in-progress', 'success', or 'failure'."""
    status: t.Optional[Status] = None

    """Cause of error"""
    error: t.Optional[str] = ""

    """Identifier for the user running the download."""
    user: str = ""

    """File size of the request in GB."""
    size: t.Optional[float] = 0

    """A UTC datetime when download was scheduled."""
    scheduled_time: t.Optional[str] = ""

    """A UTC datetime when the retrieve stage starts."""
    retrieve_start_time: t.Optional[str] = ""

    """A UTC datetime when the retrieve state ends."""
    retrieve_end_time: t.Optional[str] = ""

    """A UTC datetime when the fetch state starts."""
    fetch_start_time: t.Optional[str] = ""

    """A UTC datetime when the fetch state ends."""
    fetch_end_time: t.Optional[str] = ""

    """A UTC datetime when the download state starts."""
    download_start_time: t.Optional[str] = ""

    """A UTC datetime when the download state ends."""
    download_end_time: t.Optional[str] = ""

    """A UTC datetime when the upload state starts."""
    upload_start_time: t.Optional[str] = ""

    """A UTC datetime when the upload state ends."""
    upload_end_time: t.Optional[str] = ""

    @classmethod
    def from_dict(cls, download_status: t.Dict) -> 'DownloadStatus':
        """Instantiate DownloadStatus dataclass from dict."""
        download_status_instance = cls()
        for key, value in download_status.items():
            if key == 'status':
                setattr(download_status_instance, key, Status(value))
            elif key == 'stage':
                setattr(download_status_instance, key, Stage(value))
            else:
                setattr(download_status_instance, key, value)
        return download_status_instance

    @classmethod
    def to_dict(cls, instance) -> t.Dict:
        """Return the fields of a dataclass instance as a manifest ingestible
        dictionary mapping of field names to field values."""
        download_status_dict = {}
        for field in dataclasses.fields(instance):
            key = field.name
            value = getattr(instance, field.name)
            if isinstance(value, Status) or isinstance(value, Stage):
                download_status_dict[key] = value.value
            elif isinstance(value, pd.Timestamp):
                download_status_dict[key] = value.isoformat()
            elif key == 'selection' or key == 'error':
                download_status_dict[key] = json.dumps(value)
            else:
                download_status_dict[key] = value
        return download_status_dict


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

    # def __post_init__(self):
    #     """Initialize the manifest."""
    #     pass

    def schedule(self, selection: t.Dict, location: str, user: str) -> None:
        """Indicate that a job has been scheduled for download.

        'scheduled' jobs occur before 'in-progress', 'success' or 'finished'.
        """
        scheduled_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat(timespec='seconds')
        self.status = DownloadStatus(
                selection=selection,
                location=location,
                user=user,
                stage=Stage.NONE,
                status=Status.SCHEDULED,
                error=None,
                size=None,
                scheduled_time=scheduled_time,
                retrieve_start_time=None,
                retrieve_end_time=None,
                fetch_start_time=None,
                fetch_end_time=None,
                download_start_time=None,
                download_end_time=None,
                upload_start_time=None,
                upload_end_time=None,
            )
        self._update(self.status)

    def _set_for_transaction(self, selection: t.Dict, location: str, user: str, stage: str) -> None:
        """Reset Manifest state in preparation for a new transaction."""
        prev_download_status = self._read(location)

        scheduled_time = prev_download_status.scheduled_time
        retrieve_start_time = prev_download_status.retrieve_start_time
        retrieve_end_time = prev_download_status.retrieve_end_time
        fetch_start_time = prev_download_status.fetch_start_time
        fetch_end_time = prev_download_status.fetch_end_time
        download_start_time = prev_download_status.download_start_time
        download_end_time = prev_download_status.download_end_time
        upload_start_time = prev_download_status.upload_start_time
        upload_end_time = prev_download_status.upload_end_time
        current_utc_time = (
            datetime.datetime.utcnow()
            .replace(tzinfo=datetime.timezone.utc)
            .isoformat(timespec='seconds')
        )

        if Stage[stage] == Stage.FETCH:
            fetch_start_time = current_utc_time
        elif Stage[stage] == Stage.RETRIEVE:
            retrieve_start_time = current_utc_time
        elif Stage[stage] == Stage.DOWNLOAD:
            download_start_time = current_utc_time
        else:
            upload_start_time = current_utc_time

        self.status = DownloadStatus(
            selection=selection,
            location=location,
            user=user,
            stage=Stage[stage],
            status=Status.IN_PROGRESS,
            error=None,
            size=None,
            scheduled_time=scheduled_time,
            retrieve_start_time=retrieve_start_time,
            retrieve_end_time=retrieve_end_time,
            fetch_start_time=fetch_start_time,
            fetch_end_time=fetch_end_time,
            download_start_time=download_start_time,
            download_end_time=download_end_time,
            upload_start_time=upload_start_time,
            upload_end_time=upload_end_time,
        )

    def __enter__(self) -> None:
        """Record 'in-progress' status of a transaction."""
        self._update(self.status)

    def __exit__(self, exc_type, exc_inst, exc_tb) -> None:
        """Record end status of a transaction as either 'success' or 'failure'."""
        if exc_type is None:
            status = 'SUCCESS'
            error = None
        else:
            status = 'FAILURE'
            # For explanation, see https://docs.python.org/3/library/traceback.html#traceback.format_exception
            error = '\n'.join(traceback.format_exception(exc_type, exc_inst, exc_tb))

        current_utc_time = (
            datetime.datetime.utcnow()
            .replace(tzinfo=datetime.timezone.utc)
            .isoformat(timespec='seconds')
        )
        retrieve_end_time = self.status.retrieve_end_time
        fetch_end_time = self.status.fetch_end_time
        download_end_time = self.status.download_end_time
        upload_end_time = self.status.upload_end_time
        size = None

        if self.status.stage == Stage.FETCH:
            fetch_end_time = current_utc_time
        elif self.status.stage == Stage.RETRIEVE:
            retrieve_end_time = current_utc_time
        elif self.status.stage == Stage.DOWNLOAD:
            download_end_time = current_utc_time
        else:
            path = self.status.location
            # In case of GCS target path.
            try:
                parsed_gcs_path = urlparse(path)
                if parsed_gcs_path.scheme != 'gs' or parsed_gcs_path.netloc == '':
                    raise ValueError(f'Invalid GCS location: {path!r}.')
                bucket_name = parsed_gcs_path.netloc
                object_name = parsed_gcs_path.path[1:]
                client = storage.Client()
                bucket = client.bucket(bucket_name)
                blob = bucket.get_blob(object_name)

                size = blob.size / (10**9) if blob else 0
            # In case of local system (--local-run).
            except ValueError as e:
                e_str = str(e)
                if not (f"Invalid GCS location: {path!r}." in e_str):
                    raise
                size = os.stat(path).st_size / (1024 ** 3)

            upload_end_time = current_utc_time

        self.status = DownloadStatus(
            selection=self.status.selection,
            location=self.status.location,
            user=self.status.user,
            stage=self.status.stage,
            status=Status[status],
            error=error,
            size=size,
            scheduled_time=self.status.scheduled_time,
            retrieve_start_time=self.status.retrieve_start_time,
            retrieve_end_time=retrieve_end_time,
            fetch_start_time=self.status.fetch_start_time,
            fetch_end_time=fetch_end_time,
            download_start_time=self.status.download_start_time,
            download_end_time=download_end_time,
            upload_start_time=self.status.upload_start_time,
            upload_end_time=upload_end_time,
        )
        self._update(self.status)

    def transact(self, selection: t.Dict, location: str, user: str, stage: str) -> 'Manifest':
        """Create a download transaction."""
        self._set_for_transaction(selection, location, user, stage)
        return self

    @abc.abstractmethod
    def _read(self, location: str) -> DownloadStatus:
        pass

    @abc.abstractmethod
    def _update(self, download_status: DownloadStatus) -> None:
        pass


class ConsoleManifest(Manifest):

    def __post_init__(self):
        self.name = urlparse(self.location).hostname

    def _read(self, location: str) -> None:
        pass

    def _update(self, download_status: DownloadStatus) -> None:
        logger.info(f'[{self.name}] {dataclasses.asdict(download_status)!r}')


class GCSManifest(Manifest):
    """Writes a JSON representation of the manifest to GCS.

    This is an append-only implementation, the latest value in the manifest
    represents the current state of a download.
    """

    # Ensure no race conditions occurs on appends to objects in GCS
    # (i.e. JSON manifests).
    _lock = threading.Lock()

    def _read(self, location: str) -> None:
        pass

    def _update(self, download_status: DownloadStatus) -> None:
        """Writes the JSON data to a manifest."""
        with GCSManifest._lock:
            with gcsio.GcsIO().open(self.location, 'a') as gcs_file:
                json.dump(DownloadStatus.to_dict(download_status), gcs_file)
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

    def _read(self, location: str) -> DownloadStatus:
        """Reads the JSON data from a manifest."""
        assert os.path.exists(self.location), f'{self.location} must exist!'
        with LocalManifest._lock:
            with open(self.location, 'r') as file:
                manifest = json.load(file)
                return DownloadStatus.from_dict(manifest.get(location, {}))

    def _update(self, download_status: DownloadStatus) -> None:
        """Writes the JSON data to a manifest."""
        assert os.path.exists(self.location), f'{self.location} must exist!'
        with LocalManifest._lock:
            with open(self.location, 'r') as file:
                manifest = json.load(file)

            status = DownloadStatus.to_dict(download_status)
            manifest[status['location']] = status

            with open(self.location, 'w') as file:
                json.dump(manifest, file)
                logger.debug('Manifest written to.')
                logger.debug(download_status)


class BQManifest(Manifest):
    """Writes a JSON representation of the manifest to BQ file.

    This is an append-only implementation, the latest value in the manifest
    represents the current state of a download.
    """

    _lock = threading.Lock()

    def __init__(self, location: Location) -> None:
        super().__init__(Location(location[5:]))
        TABLE_SCHEMA = [
            bigquery.SchemaField('selection', 'JSON', mode='REQUIRED'),
            bigquery.SchemaField('location', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('stage', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('status', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('error', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('user', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('size', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('scheduled_time', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('retrieve_start_time', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('retrieve_end_time', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('fetch_start_time', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('fetch_end_time', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('download_start_time', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('download_end_time', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('upload_start_time', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('upload_end_time', 'TIMESTAMP', mode='NULLABLE'),
        ]
        table = bigquery.Table(self.location, schema=TABLE_SCHEMA)
        with bigquery.Client() as client:
            client.create_table(table, exists_ok=True)

    def _read(self, location: str) -> DownloadStatus:
        """Reads the JSON data from a manifest."""
        with BQManifest._lock:
            with bigquery.Client() as client:
                query = f"SELECT * FROM {self.location} WHERE location = {location!r}"
                query_job = client.query(query)
                result = query_job.result().to_dataframe().to_dict('records')
                row = {n: to_json_serializable_type(v) for n, v in result[0].items()}
                return DownloadStatus.from_dict(row)

    def _update(self, download_status: DownloadStatus) -> None:
        """Writes the JSON data to a manifest."""
        with BQManifest._lock:
            with bigquery.Client() as client:
                status = DownloadStatus.to_dict(download_status)
                table = client.get_table(self.location)
                columns = [field.name for field in table.schema]
                update_dml = []
                insert_dml = []
                for col in columns:
                    if status[col] is None:
                        update_dml.append(f"{col} = null")
                        insert_dml.append("null")
                    elif col == 'selection':
                        update_dml.append(f"{col} = JSON'{status[col]}'")
                        insert_dml.append(f"JSON'{status[col]}'")
                    elif col == 'size' or col == 'error':
                        update_dml.append(f"{col} = {status[col]}")
                        insert_dml.append(status[col])
                    else:
                        update_dml.append(f"{col} = '{status[col]}'")
                        insert_dml.append(f"'{status[col]}'")

                # Build the merge statement as a string
                merge_statement = f"""
                    MERGE {self.location} T
                    USING (
                    SELECT
                        '{status['location']}' as location
                    ) S
                    ON T.location = S.location
                    WHEN MATCHED THEN
                    UPDATE SET
                        {', '.join(str(v) for v in update_dml)}
                    WHEN NOT MATCHED THEN
                    INSERT
                        ({", ".join(columns)})
                    VALUES
                        ({', '.join(str(v) for v in insert_dml)})
                """

                logger.debug(merge_statement)
                # Execute the merge statement
                query_job = client.query(merge_statement)
                query_job.result()

                logger.debug('Manifest written to.')
                logger.debug(download_status)


class MockManifest(Manifest):
    """In-memory mock manifest."""

    def __init__(self, location: Location) -> None:
        super().__init__(location)
        self.records = {}

    def _read(self, location: str) -> None:
        pass

    def _update(self, download_status: DownloadStatus) -> None:
        self.records.update({dataclasses.asdict(download_status).get('location'): dataclasses.asdict(download_status)})
        logger.debug('Manifest updated.')
        logger.debug(download_status)


class NoOpManifest(Manifest):
    """A manifest that performs no operations."""

    def _read(self, location: str) -> None:
        pass

    def _update(self, download_status: DownloadStatus) -> None:
        pass


"""Exposed manifest implementations.

Users can choose their preferred manifest implementation by via the protocol of the Manifest Location.
The protocol corresponds to the keys of this ordered dictionary.

If no protocol is specified, we assume the user wants to write to the local file system.
If no key is found, the `NoOpManifest` option will be chosen. See `parsers:parse_manifest_location`.
"""
MANIFESTS = collections.OrderedDict({
    'cli': ConsoleManifest,
    'gs': GCSManifest,
    'bq': BQManifest,
    'noop': NoOpManifest,
    'mock': MockManifest,
    '': LocalManifest,
})

if __name__ == '__main__':
    # Execute doc tests
    import doctest

    doctest.testmod()
