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
"""Pipeline for reflecting lots of NetCDF objects into a BigQuery table."""

import argparse
import datetime
import json
import logging
import typing as t

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from .bq import ToBigQuery
from .streaming import GroupMessagesByFixedWindows, ParsePaths

logger = logging.getLogger(__name__)


def configure_logger(verbosity: int) -> None:
    """Configures logging from verbosity. Default verbosity will show errors."""
    level = (40 - verbosity * 10)
    logging.getLogger(__package__).setLevel(level)
    logger.setLevel(level)


def pattern_to_uris(match_pattern: str) -> t.Iterable[str]:
    for match in FileSystems().match([match_pattern]):
        yield from [x.path for x in match.metadata_list]


def pipeline(known_args: argparse.Namespace, pipeline_args: t.List[str]) -> None:
    all_uris = list(pattern_to_uris(known_args.uris))
    if not all_uris:
        raise FileNotFoundError(f"File prefix '{known_args.uris}' matched no objects")

    pipeline_options = PipelineOptions(pipeline_args)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        if known_args.topic:
            paths = (
                    p
                    # Windowing is based on this code sample:
                    # https://cloud.google.com/pubsub/docs/pubsub-dataflow#code_sample
                    | 'ReadUploadEvent' >> beam.io.ReadFromPubSub(known_args.topic)
                    | 'WindowInto' >> GroupMessagesByFixedWindows(known_args.window_size, known_args.num_shards)
                    | 'ParsePaths' >> beam.ParDo(ParsePaths(known_args.uris))
            )
        else:
            paths = p | 'Create' >> beam.Create(all_uris)

        paths | "MoveToBigQuery" >> ToBigQuery.from_kwargs(example_uri=next(iter(all_uris)), **vars(known_args))

    logger.info('Pipeline is finished.')


def run(argv: t.List[str]) -> t.Tuple[argparse.Namespace, t.List[str]]:
    """Main entrypoint & pipeline definition."""
    parser = argparse.ArgumentParser(
        prog='weather-mv',
        description='Weather Mover loads weather data from cloud storage into Google BigQuery.'
    )
    parser.add_argument('-i', '--uris', type=str, required=True,
                        help="URI prefix matching input netcdf objects, e.g. 'gs://ecmwf/era5/era5-2015-'.")
    parser.add_argument('-o', '--output_table', type=str, required=True,
                        help="Full name of destination BigQuery table (<project>.<dataset>.<table>). Table will be "
                             "created if it doesn't exist.")
    parser.add_argument('-v', '--variables', metavar='variables', type=str, nargs='+', default=list(),
                        help='Target variables (or coordinates) for the BigQuery schema. Default: will import all '
                             'data variables as columns.')
    parser.add_argument('-a', '--area', metavar='area', type=int, nargs='+', default=list(),
                        help='Target area in [N, W, S, E]. Default: Will include all available area.')
    parser.add_argument('--topic', type=str,
                        help="A Pub/Sub topic for GCS OBJECT_FINALIZE events, or equivalent, of a cloud bucket. "
                             "E.g. 'projects/<PROJECT_ID>/topics/<TOPIC_ID>'.")
    parser.add_argument("--window_size", type=float, default=1.0,
                        help="Output file's window size in minutes. Only used with the `topic` flag. Default: 1.0 "
                             "minute.")
    parser.add_argument('--num_shards', type=int, default=5,
                        help='Number of shards to use when writing windowed elements to cloud storage. Only used with '
                             'the `topic` flag. Default: 5 shards.')
    parser.add_argument('--import_time', type=str, default=datetime.datetime.utcnow().isoformat(),
                        help=("When writing data to BigQuery, record that data import occurred at this "
                              "time (format: YYYY-MM-DD HH:MM:SS.usec+offset). Default: now in UTC."))
    parser.add_argument('--infer_schema', action='store_true', default=False,
                        help='Download one file in the URI pattern and infer a schema from that file. Default: off')
    parser.add_argument('--xarray_open_dataset_kwargs', type=json.loads, default='{}',
                        help='Keyword-args to pass into `xarray.open_dataset()` in the form of a JSON string.')
    parser.add_argument('-d', '--dry-run', action='store_true', default=False,
                        help='Preview the load into BigQuery. Default: off')

    known_args, pipeline_args = parser.parse_known_args(argv[1:])

    configure_logger(2)  # 0 = error, 1 = warn, 2 = info, 3 = debug

    if known_args.area:
        assert len(known_args.area) == 4, 'Must specify exactly 4 lat/long values for area: N, W, S, E boundaries.'

    # If a topic is used, then the pipeline must be a streaming pipeline.
    if known_args.topic:
        pipeline_args.extend('--streaming true'.split())

        # make sure we re-compute utcnow() every time rows are extracted from a file.
        known_args.import_time = None

    return known_args, pipeline_args
