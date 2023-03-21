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
import enum
import hashlib
import json
import logging
import os
import pandas as pd
import threading
import time
import traceback
import typing as t
from urllib.parse import urlparse, parse_qsl

from .util import (
    to_json_serializable_type,
    fetch_geo_polygon,
    get_file_size,
    retry_with_exponential_backoff
)

import firebase_admin
from firebase_admin import firestore
from google.cloud import bigquery
from google.cloud.firestore_v1 import DocumentReference
from google.cloud.firestore_v1.types import WriteResult

"""An implementation-dependent Manifest URI."""
Location = t.NewType('Location', str)

logger = logging.getLogger(__name__)

GLOBAL_COVERAGE_AREA = [90, -180, -90, 180]


class ManifestException(Exception):
    """Errors that occur in Manifest Clients."""
    pass


class Stage(enum.Enum):
    """A request can be either in one of the following stages at a time:

    fetch : This represents request is currently in fetch stage i.e. request placed on the client's server
        & waiting for some result before starting download (eg. MARS client).
    download : This represents request is currently in download stage i.e. data is being downloading from client's
        server to the worker's local file system.
    upload : This represents request is currently in upload stage i.e. data is getting uploaded from worker's local
        file system to target location (GCS path).
    retrieve : In case of clients where there is no proper separation of fetch & download stages (eg. CDS client),
        request will be in the retrieve stage i.e. fetch + download.
    """
    RETRIEVE = 'retrieve'
    FETCH = 'fetch'
    DOWNLOAD = 'download'
    UPLOAD = 'upload'


class Status(enum.Enum):
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

    """The name of the config file associated with the request."""
    config_name: str = ""

    """Copy of selection section of the configuration."""
    selection: t.Dict = dataclasses.field(default_factory=dict)

    """Location of the downloaded data."""
    location: str = ""

    """Represents area covered by the shard."""
    area: str = ""

    """Current stage of request : 'fetch', 'download', 'retrieve', 'upload' or None."""
    stage: t.Optional[Stage] = None

    """Download status: 'scheduled', 'in-progress', 'success', or 'failure'."""
    status: t.Optional[Status] = None

    """Cause of error, if any."""
    error: t.Optional[str] = ""

    """Identifier for the user running the download."""
    username: str = ""

    """Shard size in GB."""
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
            elif key == 'stage' and value is not None:
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
            elif key == 'selection' and value is not None:
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
    # To reduce the impact of _read() and _update() calls
    # on the start time of the stage.
    prev_stage_precise_start_time: t.Optional[str] = None
    status: t.Optional[DownloadStatus] = None

    # This is overridden in subclass.
    def __post_init__(self):
        """Initialize the manifest."""
        pass

    def schedule(self, config_name: str, selection: t.Dict, location: str, user: str) -> None:
        """Indicate that a job has been scheduled for download.

        'scheduled' jobs occur before 'in-progress', 'success' or 'finished'.
        """
        scheduled_time = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat(timespec='seconds')
        self.status = DownloadStatus(
                config_name=config_name,
                selection=selection,
                location=location,
                area=fetch_geo_polygon(selection.get('area', GLOBAL_COVERAGE_AREA)),
                username=user,
                stage=None,
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

    def skip(self, config_name: str, selection: t.Dict, location: str, user: str) -> None:
        """Updates the manifest to mark the shards that were skipped in the current job
        as 'upload' stage and 'success' status, indicating that they have already been downloaded.
        """
        old_status = self._read(location)
        # The manifest needs to be updated for a skipped shard if its entry is not present, or
        # if the stage is not 'upload', or if the stage is 'upload' but the status is not 'success'.
        if old_status.location != location or old_status.stage != Stage.UPLOAD or old_status.status != Status.SUCCESS:
            current_utc_time = (
                datetime.datetime.utcnow()
                .replace(tzinfo=datetime.timezone.utc)
                .isoformat(timespec='seconds')
            )

            size = get_file_size(location)

            status = DownloadStatus(
                    config_name=config_name,
                    selection=selection,
                    location=location,
                    area=fetch_geo_polygon(selection.get('area', GLOBAL_COVERAGE_AREA)),
                    username=user,
                    stage=Stage.UPLOAD,
                    status=Status.SUCCESS,
                    error=None,
                    size=size,
                    scheduled_time=None,
                    retrieve_start_time=None,
                    retrieve_end_time=None,
                    fetch_start_time=None,
                    fetch_end_time=None,
                    download_start_time=None,
                    download_end_time=None,
                    upload_start_time=current_utc_time,
                    upload_end_time=current_utc_time,
                )
            self._update(status)
            logger.info(f'Manifest updated for skipped shard: {location!r} -- {DownloadStatus.to_dict(status)!r}.')

    def _set_for_transaction(self, config_name: str, selection: t.Dict, location: str, user: str) -> None:
        """Reset Manifest state in preparation for a new transaction."""
        self.status = dataclasses.replace(self._read(location))
        self.status.config_name = config_name
        self.status.selection = selection
        self.status.location = location
        self.status.username = user

    def __enter__(self) -> None:
        pass

    def __exit__(self, exc_type, exc_inst, exc_tb) -> None:
        """Record end status of a transaction as either 'success' or 'failure'."""
        if exc_type is None:
            status = Status.SUCCESS
            error = None
        else:
            status = Status.FAILURE
            # For explanation, see https://docs.python.org/3/library/traceback.html#traceback.format_exception
            error = '\n'.join(traceback.format_exception(exc_type, exc_inst, exc_tb))

        new_status = dataclasses.replace(self.status)
        new_status.error = error
        new_status.status = status
        current_utc_time = (
            datetime.datetime.utcnow()
            .replace(tzinfo=datetime.timezone.utc)
            .isoformat(timespec='seconds')
        )

        # This is necessary for setting the precise start time of the previous stage
        # and end time of the final stage, as well as handling the case of Status.FAILURE.
        if new_status.stage == Stage.FETCH:
            new_status.fetch_start_time = self.prev_stage_precise_start_time
            new_status.fetch_end_time = current_utc_time
        elif new_status.stage == Stage.RETRIEVE:
            new_status.retrieve_start_time = self.prev_stage_precise_start_time
            new_status.retrieve_end_time = current_utc_time
        elif new_status.stage == Stage.DOWNLOAD:
            new_status.download_start_time = self.prev_stage_precise_start_time
            new_status.download_end_time = current_utc_time
        else:
            new_status.upload_start_time = self.prev_stage_precise_start_time
            new_status.upload_end_time = current_utc_time

        new_status.size = get_file_size(new_status.location)

        self.status = new_status

        self._update(self.status)

    def transact(self, config_name: str, selection: t.Dict, location: str, user: str) -> 'Manifest':
        """Create a download transaction."""
        self._set_for_transaction(config_name, selection, location, user)
        return self

    def set_stage(self, stage: Stage) -> None:
        """Sets the current stage in manifest."""
        prev_stage = self.status.stage
        new_status = dataclasses.replace(self.status)
        new_status.stage = stage
        new_status.status = Status.IN_PROGRESS
        current_utc_time = (
            datetime.datetime.utcnow()
            .replace(tzinfo=datetime.timezone.utc)
            .isoformat(timespec='seconds')
        )

        if stage == Stage.FETCH:
            new_status.fetch_start_time = current_utc_time
        elif stage == Stage.RETRIEVE:
            new_status.retrieve_start_time = current_utc_time
        elif stage == Stage.DOWNLOAD:
            new_status.fetch_start_time = self.prev_stage_precise_start_time
            new_status.fetch_end_time = current_utc_time
            new_status.download_start_time = current_utc_time
        else:
            if prev_stage == Stage.DOWNLOAD:
                new_status.download_start_time = self.prev_stage_precise_start_time
                new_status.download_end_time = current_utc_time
            else:
                new_status.retrieve_start_time = self.prev_stage_precise_start_time
                new_status.retrieve_end_time = current_utc_time
            new_status.upload_start_time = current_utc_time

        self.status = new_status
        self._update(self.status)

    @abc.abstractmethod
    def _read(self, location: str) -> DownloadStatus:
        pass

    @abc.abstractmethod
    def _update(self, download_status: DownloadStatus) -> None:
        pass


class ConsoleManifest(Manifest):

    def __post_init__(self):
        self.name = urlparse(self.location).hostname

    def _read(self, location: str) -> DownloadStatus:
        return DownloadStatus()

    def _update(self, download_status: DownloadStatus) -> None:
        logger.info(f'[{self.name}] {DownloadStatus.to_dict(download_status)!r}')


class LocalManifest(Manifest):
    """Writes a JSON representation of the manifest to local file."""

    _lock = threading.Lock()

    def __init__(self, location: Location) -> None:
        super().__init__(Location(os.path.join(location, 'manifest.json')))
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
    def __init__(self, location: Location) -> None:
        super().__init__(Location(location[5:]))
        TABLE_SCHEMA = [
            bigquery.SchemaField('config_name', 'STRING', mode='REQUIRED',
                                 description="The name of the config file associated with the request."),
            bigquery.SchemaField('selection', 'JSON', mode='REQUIRED',
                                 description="Copy of selection section of the configuration."),
            bigquery.SchemaField('location', 'STRING', mode='REQUIRED',
                                 description="Location of the downloaded data."),
            bigquery.SchemaField('area', 'STRING', mode='NULLABLE',
                                 description="Represents area covered by the shard. "
                                 "ST_GeogFromGeoJson(area): To convert a GeoJSON geometry object into a "
                                 "GEOGRAPHY value. "
                                 "ST_COVERS(geography_expression, ST_GEOGPOINT(longitude, latitude)): To check "
                                 "if a point lies in the given area or not."),
            bigquery.SchemaField('stage', 'STRING', mode='NULLABLE',
                                 description="Current stage of request : 'fetch', 'download', 'retrieve', 'upload' "
                                 "or None."),
            bigquery.SchemaField('status', 'STRING', mode='REQUIRED',
                                 description="Download status: 'scheduled', 'in-progress', 'success', or 'failure'."),
            bigquery.SchemaField('error', 'STRING', mode='NULLABLE',
                                 description="Cause of error, if any."),
            bigquery.SchemaField('username', 'STRING', mode='REQUIRED',
                                 description="Identifier for the user running the download."),
            bigquery.SchemaField('size', 'FLOAT', mode='NULLABLE',
                                 description="Shard size in GB."),
            bigquery.SchemaField('scheduled_time', 'TIMESTAMP', mode='NULLABLE',
                                 description="A UTC datetime when download was scheduled."),
            bigquery.SchemaField('retrieve_start_time', 'TIMESTAMP', mode='NULLABLE',
                                 description="A UTC datetime when the retrieve stage starts."),
            bigquery.SchemaField('retrieve_end_time', 'TIMESTAMP', mode='NULLABLE',
                                 description="A UTC datetime when the retrieve state ends."),
            bigquery.SchemaField('fetch_start_time', 'TIMESTAMP', mode='NULLABLE',
                                 description="A UTC datetime when the fetch state starts."),
            bigquery.SchemaField('fetch_end_time', 'TIMESTAMP', mode='NULLABLE',
                                 description="A UTC datetime when the fetch state ends."),
            bigquery.SchemaField('download_start_time', 'TIMESTAMP', mode='NULLABLE',
                                 description="A UTC datetime when the download state starts."),
            bigquery.SchemaField('download_end_time', 'TIMESTAMP', mode='NULLABLE',
                                 description="A UTC datetime when the download state ends."),
            bigquery.SchemaField('upload_start_time', 'TIMESTAMP', mode='NULLABLE',
                                 description="A UTC datetime when the upload state starts."),
            bigquery.SchemaField('upload_end_time', 'TIMESTAMP', mode='NULLABLE',
                                 description="A UTC datetime when the upload state ends."),
        ]
        table = bigquery.Table(self.location, schema=TABLE_SCHEMA)
        with bigquery.Client() as client:
            client.create_table(table, exists_ok=True)

    def _read(self, location: str) -> DownloadStatus:
        """Reads the JSON data from a manifest."""
        with bigquery.Client() as client:
            select_statement = f"SELECT * FROM {self.location} WHERE location = @location"

            # Build the QueryJobConfig object with the parameters.
            job_config = bigquery.QueryJobConfig()
            job_config.query_parameters = [bigquery.ScalarQueryParameter('location', 'STRING', location)]

            # Execute the merge statement with the parameters.
            query_job = client.query(select_statement, job_config=job_config)

            # Wait for the query to execute.
            result = query_job.result()
            row = {}
            if result.total_rows > 0:
                records = result.to_dataframe().to_dict('records')
                row = {n: to_json_serializable_type(v) for n, v in records[0].items()}
            return DownloadStatus.from_dict(row)

    # Added retry here to handle the concurrency issue in BigQuery.
    # Eg: 400 Resources exceeded during query execution: Too many DML statements outstanding
    # against table <table-name>, limit is 20
    @retry_with_exponential_backoff
    def _update(self, download_status: DownloadStatus) -> None:
        """Writes the JSON data to a manifest."""
        with bigquery.Client() as client:
            status = DownloadStatus.to_dict(download_status)
            table = client.get_table(self.location)
            columns = [field.name for field in table.schema]
            parameter_type_mapping = {field.name: field.field_type for field in table.schema}

            update_dml = [f"{col} = @{col}" for col in columns]
            insert_dml = [f"@{col}" for col in columns]
            params = {col: status[col] for col in columns}

            # Build the merge statement as a string with parameter placeholders.
            merge_statement = f"""
                MERGE {self.location} T
                USING (
                SELECT
                    @location as location
                ) S
                ON T.location = S.location
                WHEN MATCHED THEN
                UPDATE SET
                    {', '.join(update_dml)}
                WHEN NOT MATCHED THEN
                INSERT
                    ({", ".join(columns)})
                VALUES
                    ({', '.join(insert_dml)})
            """

            logger.debug(merge_statement)

            # Build the QueryJobConfig object with the parameters.
            job_config = bigquery.QueryJobConfig()
            job_config.query_parameters = [bigquery.ScalarQueryParameter(col, parameter_type_mapping[col], value)
                                           for col, value in params.items()]

            # Execute the merge statement with the parameters.
            query_job = client.query(merge_statement, job_config=job_config)

            # Wait for the query to execute.
            query_job.result()

            logger.debug('Manifest written to.')
            logger.debug(download_status)


def get_wait_interval(num_retries: int = 0) -> float:
    """Returns next wait interval in seconds, using an exponential backoff algorithm."""
    if 0 == num_retries:
        return 0
    return 2 ** num_retries


class FirestoreManifest(Manifest):
    """A Firestore Manifest.
    This Manifest implementation stores DownloadStatuses in a Firebase document store.
    The document hierarchy for the manifest is as follows:
      [manifest  <or manifest name, configurable from CLI>]
      ├── doc_id (md5 hash of the path) { 'selection': {...}, 'location': ..., 'username': ... }
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

    def _read(self, location: str) -> DownloadStatus:
        """Reads the JSON data from a manifest."""

        doc_id = hashlib.md5(location.encode('utf-8')).hexdigest()

        # Update document with download status
        download_doc_ref = (
            self.root_document_for_store(doc_id)
        )

        result = download_doc_ref.get()
        row = {}
        if result.exists:
            records = result.to_dict()
            row = {n: to_json_serializable_type(v) for n, v in records.items()}
        return DownloadStatus.from_dict(row)

    def _update(self, download_status: DownloadStatus) -> None:
        """Update or create a download status record."""
        logger.debug('Updating Firestore Manifest.')

        status = DownloadStatus.to_dict(download_status)
        doc_id = hashlib.md5(status['location'].encode('utf-8')).hexdigest()

        # Update document with download status
        download_doc_ref = (
            self.root_document_for_store(doc_id)
        )

        result: WriteResult = download_doc_ref.set(status)

        logger.debug(f'Firestore manifest updated. '
                     f'update_time={result.update_time}, '
                     f'filename={download_status.location}.')

    def root_document_for_store(self, store_scheme: str) -> DocumentReference:
        """Get the root manifest document given the user's config and current document's storage location."""
        # Get user-defined collection for manifest.
        root_collection = self.get_firestore_config().get('collection', 'manifest')
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


class MockManifest(Manifest):
    """In-memory mock manifest."""

    def __init__(self, location: Location) -> None:
        super().__init__(location)
        self.records = {}

    def _read(self, location: str) -> DownloadStatus:
        manifest = self.records
        return DownloadStatus.from_dict(manifest.get(location, {}))

    def _update(self, download_status: DownloadStatus) -> None:
        status = DownloadStatus.to_dict(download_status)
        self.records.update({status.get('location'): status})
        logger.debug('Manifest updated.')
        logger.debug(download_status)


class NoOpManifest(Manifest):
    """A manifest that performs no operations."""

    def _read(self, location: str) -> DownloadStatus:
        return DownloadStatus()

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
    'fs': FirestoreManifest,
    'bq': BQManifest,
    '': LocalManifest,
})

if __name__ == '__main__':
    # Execute doc tests
    import doctest

    doctest.testmod()
