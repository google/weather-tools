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
"""Pipeline for loading weather data into analysis-ready mediums, like Google BigQuery."""

import argparse
import json
import logging
import typing as t
import warnings

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions

from .bq import ToBigQuery
from .regrid import Regrid
from .streaming import GroupMessagesByFixedWindows, ParsePaths

logger = logging.getLogger(__name__)
SDK_CONTAINER_IMAGE = 'gcr.io/weather-tools-prod/weather-tools:0.0.0'


def configure_logger(verbosity: int) -> None:
    """Configures logging from verbosity. Default verbosity will show errors."""
    level = (40 - verbosity * 10)
    logging.getLogger(__package__).setLevel(level)
    logger.setLevel(level)


def pattern_to_uris(match_pattern: str, is_zarr: bool = False) -> t.Iterable[str]:
    if is_zarr:
        yield match_pattern
        return

    for match in FileSystems().match([match_pattern]):
        yield from [x.path for x in match.metadata_list]


def pipeline(known_args: argparse.Namespace, pipeline_args: t.List[str]) -> None:
    all_uris = list(pattern_to_uris(known_args.uris, known_args.zarr))
    if not all_uris:
        raise FileNotFoundError(f"File pattern '{known_args.uris}' matched no objects")

    # First URI is useful to get an example data shard. It also can be a Zarr path.
    known_args.first_uri = next(iter(all_uris))

    with beam.Pipeline(argv=pipeline_args) as p:
        if known_args.zarr:
            paths = p
        elif known_args.topic or known_args.subscription:
            paths = (
                    p
                    # Windowing is based on this code sample:
                    # https://cloud.google.com/pubsub/docs/pubsub-dataflow#code_sample
                    | 'ReadUploadEvent' >> beam.io.ReadFromPubSub(known_args.topic, known_args.subscription)
                    | 'WindowInto' >> GroupMessagesByFixedWindows(known_args.window_size, known_args.num_shards)
                    | 'ParsePaths' >> beam.ParDo(ParsePaths(known_args.uris))
            )
        else:
            paths = p | 'Create' >> beam.Create(all_uris)

        if known_args.subcommand == 'bigquery' or known_args.subcommand == 'bq':
            paths | "MoveToBigQuery" >> ToBigQuery.from_kwargs(**vars(known_args))
        elif known_args.subcommand == 'regrid' or known_args.subcommand == 'rg':
            paths | "Regrid" >> Regrid.from_kwargs(**vars(known_args))
        elif known_args.subcommand == 'earthengine' or known_args.subcommand == 'ee':
            from .ee import ToEarthEngine
            pipeline_options = PipelineOptions(pipeline_args)
            pipeline_options_dict = pipeline_options.get_all_options()
            # all_args stores all arguments passed to the pipeline.
            # This is necessary because pipeline_args are later used by
            # the CreateTimeSeries DoFn in the AddMetrics transform.
            all_args = {**pipeline_options_dict, **vars(known_args)}
            paths | "MoveToEarthEngine" >> ToEarthEngine.from_kwargs(**all_args)
        else:
            raise ValueError('invalid subcommand!')

    logger.info('Pipeline is finished.')


def run(argv: t.List[str]) -> t.Tuple[argparse.Namespace, t.List[str]]:
    """Main entrypoint & pipeline definition."""
    parser = argparse.ArgumentParser(
        prog='weather-mv',
        description='Weather Mover loads weather data from cloud storage into analytics engines.'
    )

    # Common arguments to all commands
    base = argparse.ArgumentParser(add_help=False)
    base.add_argument('-i', '--uris', type=str, required=True,
                      help="URI glob pattern matching input weather data, e.g. 'gs://ecmwf/era5/era5-2015-*.gb'. Or, "
                           "a path to a Zarr.")
    base.add_argument('--topic', type=str,
                      help="A Pub/Sub topic for GCS OBJECT_FINALIZE events, or equivalent, of a cloud bucket. "
                           "E.g. 'projects/<PROJECT_ID>/topics/<TOPIC_ID>'. Cannot be used with `--subscription`.")
    base.add_argument('--subscription', type=str,
                      help='A Pub/Sub subscription for GCS OBJECT_FINALIZE events, or equivalent, of a cloud bucket. '
                           'Cannot be used with `--topic`.')
    base.add_argument("--window_size", type=float, default=1.0,
                      help="Output file's window size in minutes. Only used with the `topic` flag. Default: 1.0 "
                           "minute.")
    base.add_argument('--num_shards', type=int, default=5,
                      help='Number of shards to use when writing windowed elements to cloud storage. Only used with '
                           'the `topic` flag. Default: 5 shards.')
    base.add_argument('--zarr', action='store_true', default=False,
                      help="Treat the input URI as a Zarr. If the URI ends with '.zarr', this will be set to True. "
                           "Default: off")
    base.add_argument('--zarr_kwargs', type=json.loads, default='{}',
                      help='Keyword arguments to pass into `xarray.open_zarr()`, as a JSON string. '
                           'Default: `{"chunks": null, "consolidated": true}`.')
    base.add_argument('-d', '--dry-run', action='store_true', default=False,
                      help='Preview the weather-mv job. Default: off')
    base.add_argument('--log-level', type=int, default=2,
                      help='An integer to configure log level. Default: 2(INFO)')
    base.add_argument('--use-local-code', action='store_true', default=False, help='Supply local code to the Runner.')

    subparsers = parser.add_subparsers(help='help for subcommand', dest='subcommand')

    # BigQuery command registration
    bq_parser = subparsers.add_parser('bigquery', aliases=['bq'], parents=[base],
                                      help='Move data into Google BigQuery')
    ToBigQuery.add_parser_arguments(bq_parser)

    # Regrid command registration
    rg_parser = subparsers.add_parser('regrid', aliases=['rg'], parents=[base],
                                      help='Copy and regrid grib data with MetView.')
    Regrid.add_parser_arguments(rg_parser)

    # EarthEngine command registration
    ee_parser = subparsers.add_parser('earthengine', aliases=['ee'], parents=[base],
                                      help='Move data into Google EarthEngine')
    ToEarthEngine.add_parser_arguments(ee_parser)

    known_args, pipeline_args = parser.parse_known_args(argv[1:])

    configure_logger(known_args.log_level)  # 0 = error, 1 = warn, 2 = info, 3 = debug

    # Validate Zarr arguments
    if known_args.uris.endswith('.zarr'):
        known_args.zarr = True

    if known_args.zarr_kwargs and not known_args.zarr:
        raise ValueError('`--zarr_kwargs` argument is only allowed with valid Zarr input URI.')

    if known_args.zarr_kwargs:
        if not known_args.zarr_kwargs.get('start_date') or not known_args.zarr_kwargs.get('end_date'):
            warnings.warn('`--zarr_kwargs` not contains both `start_date` and `end_date`'
                          'so whole zarr-dataset will ingested.')

    if known_args.zarr:
        known_args.zarr_kwargs['chunks'] = known_args.zarr_kwargs.get('chunks', None)
        known_args.zarr_kwargs['consolidated'] = known_args.zarr_kwargs.get('consolidated', True)

    # Validate subcommand
    if known_args.subcommand == 'bigquery' or known_args.subcommand == 'bq':
        ToBigQuery.validate_arguments(known_args, pipeline_args)
    elif known_args.subcommand == 'regrid' or known_args.subcommand == 'rg':
        Regrid.validate_arguments(known_args, pipeline_args)
    elif known_args.subcommand == 'earthengine' or known_args.subcommand == 'ee':
        ToEarthEngine.validate_arguments(known_args, pipeline_args)

    # If a Pub/Sub is used, then the pipeline must be a streaming pipeline.
    if known_args.topic or known_args.subscription:
        if known_args.topic and known_args.subscription:
            raise ValueError('only one argument can be provided at a time: `topic` or `subscription`.')

        if known_args.zarr:
            raise ValueError('streaming updates to a Zarr file is not (yet) supported.')

        pipeline_args.extend('--streaming true'.split())

        # make sure we re-compute utcnow() every time rows are extracted from a file.
        known_args.import_time = None

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_args.extend('--save_main_session true'.split())

    return known_args, pipeline_args
