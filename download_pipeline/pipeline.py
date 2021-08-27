"""Primary ECMWF Downloader Workflow."""

import argparse
import copy as cp
import getpass
import itertools
import logging
import os
import tempfile
import typing as t

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, WorkerOptions, StandardOptions

from .clients import CLIENTS
from .manifest import Manifest, Location, NoOpManifest, LocalManifest
from .parsers import process_config, parse_manifest_location, use_date_as_directory
from .stores import Store, TempFileStore, GcsStore, LocalFileStore

logger = logging.getLogger(__name__)


def configure_logger(verbosity: int) -> None:
    """Configures logging from verbosity. Default verbosity will show errors."""
    level = 40 - verbosity * 10
    logging.getLogger(__package__).setLevel(level)
    logging.getLogger(__name__).setLevel(logging.INFO)


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


def skip_partition(config: t.Dict, store: Store) -> bool:
    """Return true if partition should be skipped."""

    if 'force_download' not in config['parameters'].keys():
        return False

    if config['parameters']['force_download']:
        return False

    target = prepare_target_name(config)
    if store.exists(target):
        logger.info(f'file {target} found, skipping.')
        return True

    return False


def prepare_partition(config: t.Dict, *, manifest: Manifest, store: t.Optional[Store] = None) -> t.Iterator[t.Dict]:
    """Iterate over client parameters, partitioning over `partition_keys`."""
    if store is None:
        store = GcsStore()
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

    # Output a config dictionary, overriding the range of values for
    # each key with the partition instance in 'selection'.
    # Continuing the example, the selection section would be:
    #   { 'foo': ..., 'year': ['2020'], 'month': ['01'], ... }
    #   { 'foo': ..., 'year': ['2020'], 'month': ['02'], ... }
    #   { 'foo': ..., 'year': ['2020'], 'month': ['03'], ... }
    #
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
    for index, (option, params) in enumerate(zip(fan_out, params_loop)):
        copy = cp.deepcopy(selection)
        out = cp.deepcopy(config)
        for idx, key in enumerate(partition_keys):
            copy[key] = [option[idx]]
        out['selection'] = copy
        out['parameters'].update(params)
        if skip_partition(out, store):
            continue

        selection = copy
        location = prepare_target_name(out)
        user = out['parameters'].get('user_id', 'unknown')
        manifest.schedule(selection, location, user)

        logger.info(f'Created partition [{index}] – {location}')

        yield out


def fetch_data(config: t.Dict,
               *,
               client_name: str,
               manifest: Manifest = NoOpManifest(Location('noop://in-memory')),
               store: t.Optional[Store] = None) -> None:
    """
    Download data from a client to a temp file, then upload to Google Cloud Storage.
    """
    if store is None:
        store = GcsStore()
    dataset = config['parameters'].get('dataset', '')
    target = prepare_target_name(config)
    selection = config['selection']
    user = config['parameters'].get('user_id', 'unknown')
    client = CLIENTS[client_name](config)

    with manifest.transact(selection, target, user):
        with tempfile.NamedTemporaryFile() as temp:
            logger.info(f'Fetching data for {target}')
            client.retrieve(dataset, selection, temp.name)

            # upload blob to gcs
            logger.info(f'Uploading to store for {target}')
            temp.seek(0)
            with store.open(target, 'wb') as dest:
                while True:
                    chunk = temp.read(8192)
                    if len(chunk) == 0:  # eof
                        break
                    dest.write(chunk)
            logger.info(f'Upload to store complete for {target}')


def run(argv: t.List[str], save_main_session: bool = True):
    """Main entrypoint & pipeline definition."""
    parser = argparse.ArgumentParser(
        prog='weather-dl',
        description='Weather Downloader downloads weather data to Google Cloud Storage.'
    )
    parser.add_argument('config', type=argparse.FileType('r', encoding='utf-8'),
                        help="path/to/config.cfg, containing client and data information. "
                             "Accepts *.cfg and *.json files.")
    parser.add_argument('-f', '--force-download', action="store_true",
                        help="Force redownload of partitions that were previously downloaded.")
    parser.add_argument('-d', '--dry-run', action='store_true', default=False,
                        help='Run pipeline steps without _actually_ downloading or writing to cloud storage.')
    parser.add_argument('-l', '--local-run', action='store_true', default=False,
                        help="Run pipeline locally, downloads to local hard drive.")
    parser.add_argument('-m', '--manifest-location', type=Location, default='fs://downloader-manifest',
                        help="Location of the manifest. Either a Firestore collection URI "
                             "('fs://<my-collection>?projectId=<my-project-id>'), a GCS bucket URI, or 'noop://<name>' "
                             "for an in-memory location.")

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

    manifest_location = known_args.manifest_location

    project_id__exists = 'project' in pipeline_options.get_all_options()
    project_id__not_set = 'projectId' not in manifest_location
    if manifest_location.startswith('fs://') and project_id__not_set and project_id__exists:
        start_char = '&' if '?' in manifest_location else '?'
        project = pipeline_options.get_all_options().get('project')
        manifest_location += f'{start_char}projectId={project}'

    client_name = config['parameters']['client']
    store = None  # will default to using GcsIO()
    config['parameters']['force_download'] = known_args.force_download
    manifest = parse_manifest_location(manifest_location)

    if pipeline_options.view_as(WorkerOptions).max_num_workers is None:
        max_num_workers = CLIENTS[client_name](config).num_workers_per_key(
            config.get('parameters', {}).get('dataset', "")) * config.get(
            'parameters', {}).get('num_api_keys', 1)
        pipeline_options.view_as(WorkerOptions).max_num_workers = max_num_workers

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

    with beam.Pipeline(options=pipeline_options) as p:
        (
                p
                | 'Create' >> beam.Create([config])
                | 'Partition' >> beam.FlatMap(prepare_partition, manifest=manifest, store=store)
                | 'FetchData' >> beam.Map(fetch_data, client_name=client_name, manifest=manifest, store=store)
        )
