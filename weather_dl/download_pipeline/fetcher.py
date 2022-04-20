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
import apache_beam as beam
import dataclasses
import io
import logging
import shutil
import tempfile
import typing as t
from apache_beam.io.gcp.gcsio import WRITE_CHUNK_SIZE

from .clients import CLIENTS, Client
from .manifest import Manifest, NoOpManifest, Location
from .parsers import prepare_target_name
from .config import Config
from .partition import skip_partition
from .stores import Store, FSStore
from .util import retry_with_exponential_backoff

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Fetcher(beam.PTransform):
    """Executes client download requests.

    Given a sequence of configs (keyed by subsection parameters and number of allowed
    requests), this will execute retrievals via each client. The keyed strucutre, the
    result of a `beam.GroupBy` operation, will ensure that all licenses and requests
    are utilized without any conflict.

    Attributes:
        client_name: The name of the download client to construct per each request.
        manifest: A manifest to keep track of the status of requests
        store: To manage where downloads are persisted.
        optimise_download: Optimised the download.
    """

    client_name: str
    manifest: Manifest = NoOpManifest(Location('noop://in-memory'))
    store: t.Optional[Store] = None
    optimise_download: bool = False

    def __post_init__(self):
        if self.store is None:
            self.store = FSStore()

    def expand(self, element):
        request = (
            element
            | 'EachPartition' >> beam.ParDo(EachPartition())
        )

        if self.optimise_download:
            downloaded_data = (
                request
                | 'Fetch' >> beam.ParDo(FetchData(self.client_name, self.manifest, self.store))
                | 'Download' >> beam.ParDo(DownloadData(self.client_name, self.manifest, self.store))
            )
        else:
            downloaded_data = (
                request
                | 'Fetch+Download' >> beam.ParDo(Retrieve(self.client_name, self.manifest, self.store))
            )
        (
            downloaded_data
            | 'Upload' >> beam.ParDo(Upload(self.client_name, self.manifest, self.store))
        )


class EachPartition(beam.DoFn):
    def process(self, element) -> t.Iterator[t.Tuple[t.Any, str]]:
        # element: Tuple[Tuple[str, int], Iterator[Config]]
        """Execute download requests one-by-one."""
        (subsection, request_idx), partitions = element
        worker_name = f'{subsection}.{request_idx}'
        logger.info(f"[{worker_name}] Starting requests...")
        for partition in partitions:
            beam.metrics.Metrics.counter('Fetcher', subsection).inc()
            yield (partition, worker_name)


@dataclasses.dataclass
class FetchData(beam.DoFn):
    client_name: str
    manifest: Manifest = NoOpManifest(Location('noop://in-memory'))
    store: t.Optional[Store] = None

    @retry_with_exponential_backoff
    def fetch(self, client: Client, dataset: str, selection: t.Dict) -> t.Dict:
        """Fetch data from download client, with retries."""
        return client.fetch(dataset, selection)

    def process(self, element) -> t.Iterator[t.Tuple[t.Dict, Config, str, str, str]]:
        """Fetch data from client."""
        config, worker_name = element

        if not config:
            return

        if skip_partition(config, self.store):
            return

        client = CLIENTS[self.client_name](config)
        target = prepare_target_name(config)

        with self.manifest.transact(config.selection, target, config.user_id):
            with tempfile.NamedTemporaryFile() as temp:
                logger.info(f'[{worker_name}] Fetching data for {target!r}.')
                result = self.fetch(client, config.dataset, config.selection)
                yield (result, config, worker_name, temp.name, target)


@dataclasses.dataclass
class DownloadData(beam.DoFn):
    client_name: str
    manifest: Manifest = NoOpManifest(Location('noop://in-memory'))
    store: t.Optional[Store] = None

    @retry_with_exponential_backoff
    def download(self, client: Client, dataset: str, result: t.Dict, dest: str) -> None:
        """Download data from download client, with retries."""
        client.download(dataset, result, dest)

    def process(self, element) -> t.Iterator[t.Tuple[Config, str, str, str]]:
        """Download data from a client to a temp file."""
        result, config, worker_name, temp_name, target = element

        client = CLIENTS[self.client_name](config)

        with self.manifest.transact(config.selection, target, config.user_id):
            logger.info(f'[{worker_name}] Downloading data for {target!r}.')
            self.download(client, config.dataset, result, temp_name)
            yield (config, worker_name, temp_name, target)


@dataclasses.dataclass
class Retrieve(beam.DoFn):
    client_name: str
    manifest: Manifest = NoOpManifest(Location('noop://in-memory'))
    store: t.Optional[Store] = None

    @retry_with_exponential_backoff
    def retrieve(self, client: Client, dataset: str, selection: t.Dict, dest: str) -> None:
        """Retrieve from download client, with retries."""
        client.retrieve(dataset, selection, dest)

    def process(self, element) -> t.Iterator[t.Tuple[Config, str, str, str]]:
        """Download data from a client to a temp file."""
        config, worker_name = element

        if not config:
            return

        if skip_partition(config, self.store):
            return

        client = CLIENTS[self.client_name](config)
        target = prepare_target_name(config)

        with self.manifest.transact(config.selection, target, config.user_id):
            with tempfile.NamedTemporaryFile() as temp:
                logger.info(f'[{worker_name}] Fetching and Downloading data for {target!r}.')
                self.retrieve(client, config.dataset, config.selection, temp.name)
                yield (config, worker_name, temp.name, target)


@dataclasses.dataclass
class Upload(beam.DoFn):
    client_name: str
    manifest: Manifest = NoOpManifest(Location('noop://in-memory'))
    store: t.Optional[Store] = None

    @retry_with_exponential_backoff
    def upload(self, src: str, dest: str) -> None:
        """Upload blob to cloud storage, with retries."""
        with io.FileIO(src, 'rb') as src_:
            with self.store.open(dest, 'wb') as dest_:
                shutil.copyfileobj(src_, dest_, WRITE_CHUNK_SIZE)

    def process(self, element) -> None:
        """Upload to Cloud Storage."""
        config, worker_name, temp_name, target = element

        with self.manifest.transact(config.selection, target, config.user_id):
            logger.info(f'[{worker_name}] Uploading to store for {target!r}.')
            self.upload(temp_name, target)

            logger.info(f'[{worker_name}] Upload to store complete for {target!r}.')
