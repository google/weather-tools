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
import dataclasses
import io
import logging
import shutil
import tempfile
import typing as t

import apache_beam as beam
from apache_beam.io.gcp.gcsio import WRITE_CHUNK_SIZE

from .clients import CLIENTS
from .manifest import Manifest, NoOpManifest, Location
from .parsers import prepare_target_name
from .stores import Store, FSStore
from .util import retry_with_exponential_backoff

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class Fetcher(beam.DoFn):
    """Executes client download requests.

    Given a sequence of configs (keyed by subsection parameters and number of allowed
    requests), this will execute retrievals via each client. The keyed strucutre, the
    result of a `beam.GroupBy` operation, will ensure that all licenses and requests
    are utilized without any conflict.

    Attributes:
        client_name: The name of the download client to construct per each request.
        manifest: A manifest to keep track of the status of requests
        store: To manage where downloads are persisted.
    """

    client_name: str
    manifest: Manifest = NoOpManifest(Location('noop://in-memory'))
    store: t.Optional[Store] = None

    def __post_init__(self):
        if self.store is None:
            self.store = FSStore()

    @retry_with_exponential_backoff
    def upload(self, src: io.FileIO, dest: str) -> None:
        """Upload blob to cloud storage, with retries."""
        with self.store.open(dest, 'wb') as dest_:
            shutil.copyfileobj(src, dest_, WRITE_CHUNK_SIZE)

    def fetch_data(self, config: t.Dict, *, worker_name: str = 'default') -> None:
        """Download data from a client to a temp file, then upload to Cloud Storage."""
        if not config:
            return

        client = CLIENTS[self.client_name](config)
        target = prepare_target_name(config)
        dataset = config['parameters'].get('dataset', '')
        selection = config['selection']
        user = config['parameters'].get('user_id', 'unknown')

        with self.manifest.transact(selection, target, user):
            with tempfile.NamedTemporaryFile() as temp:
                logger.info(f'[{worker_name}] Fetching data for {target!r}.')
                client.retrieve(dataset, selection, temp.name)

                logger.info(f'[{worker_name}] Uploading to store for {target!r}.')
                self.upload(temp, target)

                logger.info(f'[{worker_name}] Upload to store complete for {target!r}.')

    def process(self, element) -> None:
        # element: Tuple[Tuple[str, int], Iterator[Config]]
        """Execute download requests one-by-one."""
        (subsection, request_idx), partitions = element
        worker_name = f'{subsection}.{request_idx}'

        logger.info(f"[{worker_name}] Starting requests...")

        for partition in partitions:
            beam.metrics.Metrics.counter('Fetcher', subsection).inc()
            self.fetch_data(partition, worker_name=worker_name)
