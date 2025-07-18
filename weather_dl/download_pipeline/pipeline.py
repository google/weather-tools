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
import dataclasses
import getpass
import itertools
import logging
import os
import typing as t

import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    WorkerOptions,
)

from .clients import CLIENTS
from .config import Config
from .fetcher import Fetcher
from .manifest import (
    Location,
    LocalManifest,
    Manifest,
    NoOpManifest,
)
from .parsers import (
    parse_manifest,
    process_config,
    get_subsections,
    validate_all_configs,
)
from .partition import PartitionConfig
from .stores import TempFileStore, LocalFileStore

logger = logging.getLogger(__name__)


def configure_logger(verbosity: int) -> None:
    """Configures logging from verbosity. Default verbosity will show errors."""
    level = 40 - verbosity * 10
    logger = logging.getLogger(__package__)
    fmt = '%(levelname)s %(asctime)s %(name)s: %(message)s'
    datefmt = '%y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.root.addHandler(handler)
    logger.setLevel(level)


@dataclasses.dataclass
class PipelineArgs:
    """Options for download pipeline.

    Attributes:
        known_args: Parsed arguments. Includes user-defined args and defaults.
        pipeline_options: The apache_beam pipeline options.
        configs: The download configs / data requests.
        client_name: The type of download client (e.g. Copernicus, Mars, or a fake).
        store: A Store, which is responsible for where downloads end up.
        manifest: A Manifest, which records download progress.
        num_requesters_per_key: Number of requests per subsection (license).
    """
    known_args: argparse.Namespace
    pipeline_options: PipelineOptions
    configs: t.List[Config]
    client_name: str
    store: None
    manifest: Manifest
    num_requesters_per_key: int


def pipeline(args: PipelineArgs) -> None:
    """Main pipeline entrypoint."""
    import builtins
    import typing as t
    logger.info(f"Using '{args.num_requesters_per_key}' requests per subsection (license).")

    subsections = get_subsections(args.configs[0])

    # Capping the max number of workers to N i.e. possible simultaneous requests + fudge factor
    if args.pipeline_options.view_as(WorkerOptions).max_num_workers is None:
        max_num_workers = len(subsections) * args.num_requesters_per_key + 10
        args.pipeline_options.view_as(WorkerOptions).max_num_workers = max_num_workers
        logger.info(f"Capped the max number of workers to '{max_num_workers}'.")

    request_idxs = {name: itertools.cycle(range(args.num_requesters_per_key)) for name, _ in subsections}

    def subsection_and_request(it: Config) -> t.Tuple[str, int]:
        subsection = it.subsection_name
        return subsection, builtins.next(request_idxs[subsection])

    subsections_cycle = itertools.cycle(subsections)

    partition = PartitionConfig(args.store,
                                subsections_cycle,
                                args.manifest,
                                args.known_args.schedule,
                                args.known_args.partition_chunks,
                                args.known_args.update_manifest,
                                len(subsections) * args.num_requesters_per_key)

    with beam.Pipeline(options=args.pipeline_options) as p:
        partitions = (
                p
                | 'Create Configs' >> beam.Create(args.configs)
                | 'Prepare Partitions' >> partition
        )
        # When the --update_manifest flag is passed, the tool will only update the manifest
        # for already downloaded shards and then exit.
        if not args.known_args.update_manifest:
            (
                partitions
                | 'GroupBy Request Limits' >> beam.GroupBy(subsection_and_request)
                | 'Fetch Data' >> beam.ParDo(Fetcher(args.client_name,
                                                     args.manifest,
                                                     args.store,
                                                     args.known_args.log_level))
            )


def run(argv: t.List[str], save_main_session: bool = True) -> PipelineArgs:
    """Parse user arguments and configure the pipeline."""
    parser = argparse.ArgumentParser(
        prog='weather-dl',
        description='Weather Downloader ingests weather data to cloud storage.'
    )
    parser.add_argument('config', type=str, nargs='+',
                        help="path/to/configs.cfg, containing client and data information. Can take multiple configs."
                             "Accepts *.cfg and *.json files.")
    parser.add_argument('-f', '--force-download', action="store_true", default=False,
                        help="Force redownload of partitions that were previously downloaded.")
    parser.add_argument('-d', '--dry-run', action='store_true', default=False,
                        help='Run pipeline steps without _actually_ downloading or writing to cloud storage.')
    parser.add_argument('-l', '--local-run', action='store_true', default=False,
                        help="Run pipeline locally, downloads to local hard drive.")
    parser.add_argument('-m', '--manifest-location', type=Location, default='cli://manifest',
                        help="Location of the manifest. By default, it will use Cloud Logging (stdout for direct "
                             "runner). You can set the name of the manifest as the hostname of a URL with the 'cli' "
                             "protocol. For example, 'cli://manifest' will prefix all the manifest logs as "
                             "'[manifest]'. In addition, users can specify either a Firestore collection URI "
                             "('fs://<my-collection>?projectId=<my-project-id>'), or BigQuery table "
                             "('bq://<project-id>.<dataset-name>.<table-name>') [Note: Tool will create the BQ table "
                             "itself, if not already present. Or it will use the existing table but can report errors "
                             "in case of schema mismatch.], or 'noop://<name>' for an in-memory location.")
    parser.add_argument('-n', '--num-requests-per-key', type=int, default=-1,
                        help='Number of concurrent requests to make per API key. '
                             'Default: make an educated guess per client & config. '
                             'Please see the client documentation for more details.')
    parser.add_argument('-p', '--partition-chunks', type=int, default=None,
                        help='Group shards into chunks of this size when computing the partitions. Specifically, '
                             'this controls how we chunk elements in a cartesian product, which affects '
                             "parallelization of that step. Default: chunks of 1000 elements for 'in-order' scheduling."
                             " Chunks of 1 element for 'fair' scheduling.")
    parser.add_argument('-s', '--schedule', choices=['in-order', 'fair'], default='in-order',
                        help="When using multiple configs, decide how partitions are scheduled: 'in-order' implies "
                             "that partitions will be processed in sequential order of each config; 'fair' means that "
                             "partitions from each config will be interspersed evenly. "
                             "Note: When using 'fair' scheduling, we recommend you set the '--partition-chunks' to a "
                             "much smaller number. Default: 'in-order'.")
    parser.add_argument('--check-skip-in-dry-run', action='store_true', default=False,
                        help="To enable file skipping logic in dry-run mode. Default: 'false'.")
    parser.add_argument('-u', '--update-manifest', action='store_true', default=False,
                        help="Update the manifest for the already downloaded shards and exit. Default: 'false'.")
    parser.add_argument('--log-level', type=int, default=2,
                        help='An integer to configure log level. Default: 2(INFO)')
    parser.add_argument('--use-local-code', action='store_true', default=False,
                        help='Supply local code to the Runner.')

    known_args, pipeline_args = parser.parse_known_args(argv[1:])

    configure_logger(known_args.log_level)  # 0 = error, 1 = warn, 2 = info, 3 = debug

    configs = []
    for cfg in known_args.config:
        with open(cfg, 'r', encoding='utf-8') as f:
            # configs/example.cfg -> example.cfg
            config_name = os.path.split(cfg)[1]
            config = process_config(f, config_name)

        config.force_download = known_args.force_download
        config.user_id = getpass.getuser()
        configs.append(config)

    # This enables support for updating just the manifest for multiple config (*.cfg)
    # when running the tool with the '-u' or '--update-manifest' flag.
    if not known_args.update_manifest:
        validate_all_configs(configs)

    if known_args.check_skip_in_dry_run and not known_args.dry_run:
        raise RuntimeError('--check-skip-in-dry-run can only be used along with --dry-run flag.')

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    save_main_session_args = ['--save_main_session'] + ['True' if save_main_session else 'False']
    pipeline_options = PipelineOptions(pipeline_args + save_main_session_args)

    client_name = config.client
    store = None  # will default to using FileSystems()
    manifest = parse_manifest(known_args.manifest_location, pipeline_options.get_all_options())

    if known_args.dry_run:
        client_name = 'fake'
        if not known_args.check_skip_in_dry_run:
            store = TempFileStore('dry_run')
            logger.warning('File skipping logic is disabled by default in dry-run mode.'
                           'To enable please pass the flag --check-skip-in-dry-run along with the'
                           'dry run flag.')
            for config in configs:
                config.force_download = True
        manifest = NoOpManifest(Location('noop://dry-run'))

    if known_args.local_run:
        local_dir = '{}/local_run'.format(os.getcwd())
        store = LocalFileStore(local_dir)
        pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'
        manifest = LocalManifest(Location(local_dir))

    num_requesters_per_key = known_args.num_requests_per_key
    known_args.log_level = 40 - known_args.log_level * 10
    client = CLIENTS[client_name](configs[0], known_args.log_level)
    if num_requesters_per_key == -1:
        num_requesters_per_key = client.num_requests_per_key(config.dataset)

    logger.warning(f'By using {client_name} datasets, '
                   f'users agree to the terms and conditions specified in {client.license_url!r}')

    return PipelineArgs(
        known_args, pipeline_options, configs, client_name, store, manifest, num_requesters_per_key
    )
