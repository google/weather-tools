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
"""Primary ECMWF Downloader Workflow."""
import argparse
import copy as cp
import getpass
import itertools
import logging
import os
import typing as t

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions,
    StandardOptions,
)

from .clients import CLIENTS
from .fetcher import Fetcher
from .manifest import Manifest, Location, NoOpManifest, LocalManifest
from .parsers import (
    Config,
    parse_manifest_location,
    prepare_target_name,
    process_config,
)
from .stores import Store, TempFileStore, FSStore, LocalFileStore

logger = logging.getLogger(__name__)

Partition = t.Tuple[str, t.Dict, Config]


def configure_logger(verbosity: int) -> None:
    """Configures logging from verbosity. Default verbosity will show errors."""
    level = 40 - verbosity * 10
    logging.getLogger(__package__).setLevel(level)
    logger.setLevel(level)


def _create_partition_config(option: t.Tuple, config: Config) -> Config:
    """Create a config for a single partition option.

    Output a config dictionary, overriding the range of values for
    each key with the partition instance in 'selection'.
    Continuing the example from prepare_partitions, the selection section
    would be:
      { 'foo': ..., 'year': ['2020'], 'month': ['01'], ... }
      { 'foo': ..., 'year': ['2020'], 'month': ['02'], ... }
      { 'foo': ..., 'year': ['2020'], 'month': ['03'], ... }

    Args:
        option: A single item in the range of partition_keys.
        config: The download config, including the parameters and selection sections.

    Returns:
        A configuration with that selects a single download partition.
    """
    partition_keys = config.get('parameters', {}).get('partition_keys', [])
    selection = config.get('selection', {})
    copy = cp.deepcopy(selection)
    out = cp.deepcopy(config)
    for idx, key in enumerate(partition_keys):
        copy[key] = [option[idx]]

    out['selection'] = copy
    return out


def skip_partition(config: Config, store: Store) -> bool:
    """Return true if partition should be skipped."""

    if config['parameters'].get('force_download', False):
        return False

    target = prepare_target_name(config)
    if store.exists(target):
        logger.info(f'file {target} found, skipping.')
        return True

    return False


def get_subsections(config: t.Dict) -> t.List[t.Tuple[str, t.Dict]]:
    """Collect parameter subsections from main configuration.

    If the `parameters` section contains subsections (e.g. '[parameters.1]',
    '[parameters.2]'), collect the subsection key-value pairs. Otherwise,
    return an empty dictionary (i.e. there are no subsections).

    This is useful for specifying multiple API keys for your configuration.
    For example:
    ```
      [parameters.alice]
      api_key=KKKKK1
      api_url=UUUUU1
      [parameters.bob]
      api_key=KKKKK2
      api_url=UUUUU2
      [parameters.eve]
      api_key=KKKKK3
      api_url=UUUUU3
    ```
    """
    return [(name, params) for name, params in config['parameters'].items()
            if isinstance(params, dict)] or [('default', {})]


def prepare_partitions(config: Config) -> t.Iterator[Config]:
    """Iterate over client parameters, partitioning over `partition_keys`.

    This produces a Cartesian-Cross over the range of keys.

    For example, if the keys were 'year' and 'month', it would produce
    an iterable like:
        ( ('2020', '01'), ('2020', '02'), ('2020', '03'), ...)
    """
    partition_keys = config.get('parameters', {}).get('partition_keys', [])
    selection = config.get('selection', {})

    for option in itertools.product(*[selection[key] for key in partition_keys]):
        yield _create_partition_config(option, config)


def new_downloads_only(candidate: Config, store: t.Optional[Store] = None) -> bool:
    """Predicate function to skip already downloaded partitions."""
    if store is None:
        store = FSStore()
    should_skip = skip_partition(candidate, store)
    if should_skip:
        beam.metrics.Metrics.counter('Prepare', 'skipped').inc()
    return not should_skip


def assemble_config(partition: Partition, manifest: Manifest) -> Config:
    """Assemble the configuration for a single partition.

    For each cross product of the 'selection' sections, the output dictionary
    will overwrite parameters from the extra param subsections, evenly cycling
    through each subsection.

    For example:
      { 'parameters': {... 'api_key': KKKKK1, ... }, ... }
      { 'parameters': {... 'api_key': KKKKK2, ... }, ... }
      { 'parameters': {... 'api_key': KKKKK3, ... }, ... }
      { 'parameters': {... 'api_key': KKKKK1, ... }, ... }
      { 'parameters': {... 'api_key': KKKKK2, ... }, ... }
      { 'parameters': {... 'api_key': KKKKK3, ... }, ... }
      ...
    """
    name, params, out = partition
    out['parameters'].update(params)
    out['parameters']['__subsection__'] = name

    location = prepare_target_name(out)
    user = out['parameters'].get('user_id', 'unknown')
    manifest.schedule(out['selection'], location, user)

    logger.info(f'[{name}] Created partition {location!r}.')
    beam.metrics.Metrics.counter('Subsection', name).inc()

    return out


def run(argv: t.List[str], save_main_session: bool = True):
    """Main entrypoint & pipeline definition."""
    parser = argparse.ArgumentParser(
        prog='weather-dl',
        description='Weather Downloader ingests weather data to cloud storage.'
    )
    parser.add_argument('config', type=argparse.FileType('r', encoding='utf-8'),
                        help="path/to/config.cfg, containing client and data information. "
                             "Accepts *.cfg and *.json files.")
    parser.add_argument('-f', '--force-download', action="store_true", default=False,
                        help="Force redownload of partitions that were previously downloaded.")
    parser.add_argument('-d', '--dry-run', action='store_true', default=False,
                        help='Run pipeline steps without _actually_ downloading or writing to cloud storage.')
    parser.add_argument('-l', '--local-run', action='store_true', default=False,
                        help="Run pipeline locally, downloads to local hard drive.")
    parser.add_argument('-m', '--manifest-location', type=Location, default='fs://downloader-manifest',
                        help="Location of the manifest. Either a Firestore collection URI "
                             "('fs://<my-collection>?projectId=<my-project-id>'), a GCS bucket URI, or 'noop://<name>' "
                             "for an in-memory location.")
    parser.add_argument('-n', '--num-requests-per-key', type=int, default=-1,
                        help='Number of concurrent requests to make per API key. '
                             'Default: make an educated guess per client & config. '
                             'Please see the client documentation for more details.')

    known_args, pipeline_args = parser.parse_known_args(argv[1:])

    configure_logger(3)  # 0 = error, 1 = warn, 2 = info, 3 = debug

    config = {}
    with known_args.config as f:
        config = process_config(f)

    config['parameters']['force_download'] = known_args.force_download
    config['parameters']['user_id'] = getpass.getuser()

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args + '--save_main_session True'.split())

    client_name = config['parameters']['client']
    store = None  # will default to using FileSystems()
    config['parameters']['force_download'] = known_args.force_download
    manifest = parse_manifest_location(known_args.manifest_location, pipeline_options.get_all_options())
    subsections = get_subsections(config)

    if known_args.dry_run:
        client_name = 'fake'
        store = TempFileStore('dry_run')
        config['parameters']['force_download'] = True
        manifest = NoOpManifest(Location('noop://dry-run'))

    if known_args.local_run:
        local_dir = '{}/local_run'.format(os.getcwd())
        store = LocalFileStore(local_dir)
        pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'
        manifest = LocalManifest(Location(local_dir))

    num_requesters_per_key = known_args.num_requests_per_key
    if num_requesters_per_key == -1:
        num_requesters_per_key = CLIENTS[client_name](config).num_requests_per_key(
            config.get('parameters', {}).get('dataset', "")
        )

    logger.info(f"Using '{num_requesters_per_key}' requests per license (subsection).")

    request_idxs = {name: itertools.cycle(range(num_requesters_per_key)) for name, _ in subsections}

    def subsection_and_request(it: Config) -> t.Tuple[str, int]:
        subsection = t.cast(str, it.get('parameters', {}).get('__subsection__', 'default'))
        return subsection, next(request_idxs[subsection])

    subsections_cycle = itertools.cycle(subsections)

    def loop_through_subsections(it: Config) -> Partition:
        """Assign a subsection to each config in a loop.

        If the `parameters` section contains subsections (e.g. '[parameters.1]',
        '[parameters.2]'), collect a repeating cycle of the subsection key-value
        pairs. Otherwise, assign a default section to each config.

        This is useful for specifying multiple API keys for your configuration.

        For example:
        ```
          [parameters.alice]
          api_key=KKKKK1
          api_url=UUUUU1
          [parameters.bob]
          api_key=KKKKK2
          api_url=UUUUU2
          [parameters.eve]
          api_key=KKKKK3
          api_url=UUUUU3
        ```
        """
        name, params = next(subsections_cycle)
        return name, params, it

    with beam.Pipeline(options=pipeline_options) as p:
        (
                p
                | 'Create the initial config' >> beam.Create([config])
                | 'Prepare partitions' >> beam.FlatMap(prepare_partitions)
                | 'Skip existing downloads' >> beam.Filter(new_downloads_only, store=store)
                | 'Cycle through subsections' >> beam.Map(loop_through_subsections)
                | 'Assemble the data request' >> beam.Map(assemble_config, manifest=manifest)
                | 'GroupBy request limits' >> beam.GroupBy(subsection_and_request)
                | 'Fetch data' >> beam.ParDo(Fetcher(client_name, manifest, store))
        )
