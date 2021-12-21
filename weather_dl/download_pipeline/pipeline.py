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
import shutil
import tempfile
import typing as t
import warnings

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    DebugOptions,
    PipelineOptions,
    SetupOptions,
    WorkerOptions,
    StandardOptions,
)

from .clients import CLIENTS
from .manifest import Manifest, Location, NoOpManifest, LocalManifest
from .parsers import process_config, parse_manifest_location, use_date_as_directory
from .stores import Store, TempFileStore, FSStore, LocalFileStore

logger = logging.getLogger(__name__)


def configure_logger(verbosity: int) -> None:
    """Configures logging from verbosity. Default verbosity will show errors."""
    level = 40 - verbosity * 10
    logging.getLogger(__package__).setLevel(level)
    logger.setLevel(level)


def configure_workers(client_name: str,
                      config: t.Dict,
                      num_requesters_per_key: int,
                      pipeline_options: PipelineOptions) -> PipelineOptions:
    """Configure the number of workers and threads for the pipeline, allowing for user control."""

    # The number of workers should always be proportional to the number of licenses.
    num_api_keys = config.get('parameters', {}).get('num_api_keys', 1)

    # If user doesn't specify a number of requestors, make educated guess based on clients and dataset.
    if num_requesters_per_key == -1:
        num_requesters_per_key = CLIENTS[client_name](config).num_requests_per_key(
            config.get('parameters', {}).get('dataset', "")
        )

    max_num_requesters = num_requesters_per_key * num_api_keys

    # Default: Assume user intends to have two thread per worker.
    if pipeline_options.view_as(DebugOptions).number_of_worker_harness_threads is None:
        pipeline_options.view_as(DebugOptions).add_experiment('use_runner_v2')
        pipeline_options.view_as(DebugOptions).number_of_worker_harness_threads = 2

    n_threads = pipeline_options.view_as(DebugOptions).number_of_worker_harness_threads
    max_num_workers_with_n_threads = max_num_requesters // n_threads + int(max_num_requesters % n_threads > 0)

    if pipeline_options.view_as(WorkerOptions).max_num_workers is None:
        pipeline_options.view_as(WorkerOptions).max_num_workers = max_num_workers_with_n_threads
        pipeline_options.view_as(WorkerOptions).num_workers = max_num_workers_with_n_threads

    if pipeline_options.view_as(WorkerOptions).max_num_workers > max_num_workers_with_n_threads:
        warnings.warn(
            f'Max number of workers {pipeline_options.view_as(WorkerOptions).max_num_workers!r} with '
            f'{n_threads!r} threads each exceeds recommended {max_num_requesters!r} concurrent requests.'
        )

    return pipeline_options


def prepare_target_name(config: t.Dict) -> str:
    """Returns name of target location."""
    target_path = config['parameters']['target_path']
    target_filename = config['parameters'].get('target_filename', '')
    partition_keys = config['parameters']['partition_keys'].copy()
    if use_date_as_directory(config):
        target_path = "{}/{}".format(
            target_path,
            ''.join(['/'.join(date_value for date_value in config['selection']['date'][0].split('-'))]))
        logger.debug(f'target_path adjusted for date: {target_path}')
        partition_keys.remove('date')
    target_path = "{}{}".format(target_path, target_filename)
    partition_key_values = [config['selection'][key][0] for key in partition_keys]
    target = target_path.format(*partition_key_values)
    logger.debug(f'target name for partition: {target}')

    return target


def _create_partition_config(option: t.Tuple, config: t.Dict) -> t.Dict:
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
    partition_keys = config['parameters']['partition_keys']
    selection = config.get('selection', {})
    copy = cp.deepcopy(selection)
    out = cp.deepcopy(config)
    for idx, key in enumerate(partition_keys):
        copy[key] = [option[idx]]

    out['selection'] = copy
    return out


def skip_partition(config: t.Dict, store: Store) -> bool:
    """Return true if partition should be skipped."""

    if config['parameters'].get('force_download', False):
        return False

    target = prepare_target_name(config)
    if store.exists(target):
        logger.info(f'file {target} found, skipping.')
        return True

    return False


def prepare_partitions(config: t.Dict, store: t.Optional[Store] = None) -> t.Iterator[t.Tuple]:
    """Iterate over client parameters, partitioning over `partition_keys`."""
    if store is None:
        store = FSStore()
    partition_keys = config['parameters']['partition_keys']
    selection = config.get('selection', {})

    # Produce a Cartesian-Cross over the range of keys.
    # For example, if the keys were 'year' and 'month', it would produce
    # an iterable like: ( ('2020', '01'), ('2020', '02'), ('2020', '03'), ...)
    fan_out = itertools.product(*[selection[key] for key in partition_keys])

    # If the `parameters` section contains subsections (e.g. '[parameters.1]',
    # '[parameters.2]'), collect a repeating cycle of the subsection key-value
    # pairs. Otherwise, store empty dictionaries.
    #
    # This is useful for specifying multiple API keys for your configuration.
    # For example:
    # ```
    #   [parameters.deepmind]
    #   api_key=KKKKK1
    #   api_url=UUUUU1
    #   [parameters.research]
    #   api_key=KKKKK2
    #   api_url=UUUUU2
    #   [parameters.cloud]
    #   api_key=KKKKK3
    #   api_url=UUUUU3
    # ```
    extra_params = [params for _, params in config['parameters'].items() if isinstance(params, dict)]
    params_loop = itertools.cycle(extra_params) if extra_params else itertools.repeat({})

    def new_downloads_only(candidate: t.Dict) -> bool:
        """Predicate function to skip already downloaded partitions."""
        return not skip_partition(candidate, store)

    return zip(
        filter(new_downloads_only, [_create_partition_config(option, config) for option in fan_out]),
        params_loop
    )


def assemble_partition_config(partition: t.Tuple,
                              config: t.Dict,
                              manifest: Manifest) -> t.Dict:
    """Assemble the configuration for a single partition."""
    # For each of these 'selection' sections, the output dictionary will
    # overwrite parameters from the extra param subsections (above),
    # evenly cycling through each subsection.
    # For example:
    #   { 'parameters': {... 'api_key': KKKKK1, ... }, ... }
    #   { 'parameters': {... 'api_key': KKKKK2, ... }, ... }
    #   { 'parameters': {... 'api_key': KKKKK3, ... }, ... }
    #   { 'parameters': {... 'api_key': KKKKK1, ... }, ... }
    #   { 'parameters': {... 'api_key': KKKKK2, ... }, ... }
    #   { 'parameters': {... 'api_key': KKKKK3, ... }, ... }
    #   ...
    out, params = partition
    out['parameters'].update(params)

    location = prepare_target_name(out)
    user = out['parameters'].get('user_id', 'unknown')
    manifest.schedule(out['selection'], location, user)

    logger.info(f'Created partition {location!r}.')
    return out


def fetch_data(config: t.Dict,
               *,
               client_name: str,
               manifest: Manifest = NoOpManifest(Location('noop://in-memory')),
               store: t.Optional[Store] = None) -> None:
    """
    Download data from a client to a temp file, then upload to Google Cloud Storage.
    """
    if not config:
        return

    if store is None:
        store = FSStore()

    client = CLIENTS[client_name](config)
    target = prepare_target_name(config)
    dataset = config['parameters'].get('dataset', '')
    selection = config['selection']
    user = config['parameters'].get('user_id', 'unknown')

    with manifest.transact(selection, target, user):
        with tempfile.NamedTemporaryFile() as temp:
            logger.info(f'Fetching data for {target!r}.')
            client.retrieve(dataset, selection, temp.name)

            # upload blob to cloud storage
            logger.info(f'Uploading to store for {target!r}.')
            with store.open(target, 'wb') as dest:
                shutil.copyfileobj(temp, dest)

            logger.info(f'Upload to store complete for {target!r}.')


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
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    client_name = config['parameters']['client']
    store = None  # will default to using FileSystems()
    config['parameters']['force_download'] = known_args.force_download
    manifest = parse_manifest_location(known_args.manifest_location, pipeline_options.get_all_options())

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

    pipeline_options = configure_workers(client_name, config, known_args.num_requests_per_key, pipeline_options)

    with beam.Pipeline(options=pipeline_options) as p:
        (
                p
                | 'Create' >> beam.Create([config])
                | 'Prepare' >> beam.FlatMap(prepare_partitions, store=store)
                # Shuffling here prevents beam from fusing all steps,
                # which would result in utilizing only a single worker.
                | 'Shuffle' >> beam.Reshuffle()
                | 'Partition' >> beam.Map(assemble_partition_config, config=config, manifest=manifest)
                | 'FetchData' >> beam.Map(fetch_data, client_name=client_name, manifest=manifest, store=store)
        )
