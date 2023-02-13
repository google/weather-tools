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
import datetime
import logging
import tempfile
import typing as t

import apache_beam as beam

from .clients import CLIENTS, Client
from .config import Config
from .manifest import Manifest, NoOpManifest, Location, Stage
from .parsers import prepare_target_name
from .partition import skip_partition
from .stores import Store, FSStore
from .util import copy, retry_with_exponential_backoff

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
    def retrieve(self, client: Client, dataset: str, selection: t.Dict, dest: str) -> None:
        """Retrieve from download client, with retries."""
        client.retrieve(dataset, selection, dest, self.manifest)

    def fetch_data(self, config: Config, *, worker_name: str = 'default') -> None:
        """Download data from a client to a temp file, then upload to Cloud Storage."""
        if not config:
            return

        if skip_partition(config, self.store):
            return

        client = CLIENTS[self.client_name](config)
        target = prepare_target_name(config)

        with tempfile.NamedTemporaryFile() as temp:
            logger.info(f'[{worker_name}] Fetching data for {target!r}.')
            with self.manifest.transact(config.kwargs.get('config_name'), config.selection, target, config.user_id):
                self.retrieve(client, config.dataset, config.selection, temp.name)

                self.manifest.set_stage(Stage.UPLOAD)
                precise_upload_start_time = (
                    datetime.datetime.utcnow()
                    .replace(tzinfo=datetime.timezone.utc)
                    .isoformat(timespec='seconds')
                )
                self.manifest.prev_stage_precise_start_time = precise_upload_start_time
                logger.info(f'[{worker_name}] Uploading to store for {target!r}.')

                # In dry-run mode we actually aren't required to upload a file.
                if not self.client_name == "fake":
                    copy(temp.name, target)

                logger.info(f'[{worker_name}] Upload to store complete for {target!r}.')

    def process(self, element) -> None:
        # element: Tuple[Tuple[str, int], Iterator[Config]]
        """Execute download requests one-by-one."""
        (subsection, request_idx), partitions = element
        worker_name = f'{subsection}.{request_idx}'

        logger.info(f'[{worker_name}] Starting requests...')
        logger.debug(f'[{worker_name}] Partitions: {partitions!r}.')

        for partition in partitions:
            beam.metrics.Metrics.counter('Fetcher', subsection).inc()
            self.fetch_data(partition, worker_name=worker_name)
