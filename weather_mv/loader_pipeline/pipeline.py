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
import tempfile
import typing as t
import signal
import sys
import uuid
import traceback
import os
from functools import partial

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from google.cloud import bigquery, storage
from google.api_core.exceptions import BadRequest, NotFound
from urllib.parse import urlparse

from .bq import ToBigQuery
from .streaming import GroupMessagesByFixedWindows, ParsePaths

CANARY_BUCKET_NAME = 'anthromet_canary_bucket'
CANARY_RECORD = {'foo': 'bar'}
CANARY_RECORD_FILE_NAME = 'canary_record.json'
CANARY_OUTPUT_TABLE_SUFFIX = '_anthromet_canary_table'
CANARY_TABLE_SCHEMA = [bigquery.SchemaField('name', 'STRING', mode='NULLABLE')]

logger = logging.getLogger(__name__)


def configure_logger(verbosity: int) -> None:
    """Configures logging from verbosity. Default verbosity will show errors."""
    level = (40 - verbosity * 10)
    logging.getLogger(__package__).setLevel(level)
    logger.setLevel(level)


def pattern_to_uris(match_pattern: str) -> t.Iterable[str]:
    for match in FileSystems().match([match_pattern]):
        yield from [x.path for x in match.metadata_list]


def _cleanup(bigquery_client: bigquery.Client, storage_client: storage.Client, canary_output_table: str,
             canay_bucket_name: str, sig: t.Optional[t.Any] = None, frame: t.Optional[t.Any] = None) -> None:
    bigquery_client.delete_table(canary_output_table, not_found_ok=True)
    try:
        storage_client.get_bucket(canay_bucket_name).delete(force=True)
    except NotFound:
        pass
    if sig:
        traceback.print_stack(frame)
        sys.exit(0)


def validate_region(output_table: str, temp_location: t.Optional[str] = None, region: t.Optional[str] = None) -> None:
    """Validates non-compatible regions scenarios by performing sanity check."""
    if not region and not temp_location:
        raise ValueError('Invalid GCS location: None.')

    bigquery_client = bigquery.Client()
    storage_client = storage.Client()
    canary_output_table = output_table + CANARY_OUTPUT_TABLE_SUFFIX + str(uuid.uuid4())
    canay_bucket_name = CANARY_BUCKET_NAME + str(uuid.uuid4())

    # Doing cleanup if operation get cut off midway.
    # TODO : Should we handle some other signals ?
    do_cleanup = partial(_cleanup, bigquery_client, storage_client, canary_output_table, canay_bucket_name)
    original_sigint_handler = signal.getsignal(signal.SIGINT)
    original_sigtstp_handler = signal.getsignal(signal.SIGTSTP)
    signal.signal(signal.SIGINT, do_cleanup)
    signal.signal(signal.SIGTSTP, do_cleanup)

    bucket_region = region
    table_region = None

    if temp_location:
        parsed_temp_location = urlparse(temp_location)
        if parsed_temp_location.scheme != 'gs' or parsed_temp_location.netloc == '':
            raise ValueError(f'Invalid GCS location: {temp_location!r}.')
        bucket_name = parsed_temp_location.netloc
        bucket_region = storage_client.get_bucket(bucket_name).location

    try:
        bucket = storage_client.create_bucket(canay_bucket_name, location=bucket_region)
        with tempfile.NamedTemporaryFile(mode='w+') as temp:
            json.dump(CANARY_RECORD, temp)
            temp.flush()
            blob = bucket.blob(CANARY_RECORD_FILE_NAME)
            blob.upload_from_filename(temp.name)

        table = bigquery.Table(canary_output_table, schema=CANARY_TABLE_SCHEMA)
        table = bigquery_client.create_table(table, exists_ok=True)
        table_region = table.location

        load_job = bigquery_client.load_table_from_uri(
            f'gs://{canay_bucket_name}/{CANARY_RECORD_FILE_NAME}',
            canary_output_table,
        )
        load_job.result()
    except BadRequest:
        raise RuntimeError(f'Can\'t migrate from source: {bucket_region} to destination: {table_region}')
    finally:
        _cleanup(bigquery_client, storage_client, canary_output_table, canay_bucket_name)
        signal.signal(signal.SIGINT, original_sigint_handler)
        signal.signal(signal.SIGINT, original_sigtstp_handler)


def pipeline(known_args: argparse.Namespace, pipeline_args: t.List[str]) -> None:
    all_uris = list(pattern_to_uris(known_args.uris))
    if not all_uris:
        raise FileNotFoundError(f"File prefix '{known_args.uris}' matched no objects")

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options_dict = pipeline_options.get_all_options()

    if not (known_args.dry_run or known_args.skip_region_validation):
        # Program execution will terminate on failure of region validation.
        logger.info('Validating regions for data migration. This might take a few seconds...')
        validate_region(known_args.output_table, temp_location=pipeline_options_dict.get('temp_location'),
                        region=pipeline_options_dict.get('region'))
        logger.info('Region validation completed successfully.')

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
    parser.add_argument('--coordinate_chunk_size', type=int, default=10_000,
                        help='The size of the chunk of coordinates used for extracting vector data into BigQuery. '
                             'Used to tune parallel uploads.')
    parser.add_argument('--tif_metadata_for_datetime', type=str, default=None,
                        help='Metadata that contains tif file\'s timestamp. '
                             'Applicable only for tif files.')
    parser.add_argument('-d', '--dry-run', action='store_true', default=False,
                        help='Preview the load into BigQuery. Default: off')
    parser.add_argument('--disable_in_memory_copy', action='store_true', default=False,
                        help="To disable in-memory copying of dataset. Default: False")
    parser.add_argument('-s', '--skip-region-validation', action='store_true', default=False,
                        help='Skip validation of regions for data migration. Default: off')

    known_args, pipeline_args = parser.parse_known_args(argv[1:])

    configure_logger(2)  # 0 = error, 1 = warn, 2 = info, 3 = debug

    _, uri_extension = os.path.splitext(known_args.uris)
    if uri_extension == '.tif' and not known_args.tif_metadata_for_datetime:
        raise RuntimeError("'--tif_metadata_for_datetime' is required for tif files.")
    elif uri_extension != '.tif' and known_args.tif_metadata_for_datetime:
        raise RuntimeError("'--tif_metadata_for_datetime' can be specified only for tif files.")

    if known_args.area:
        assert len(known_args.area) == 4, 'Must specify exactly 4 lat/long values for area: N, W, S, E boundaries.'

    # If a topic is used, then the pipeline must be a streaming pipeline.
    if known_args.topic:
        pipeline_args.extend('--streaming true'.split())

        # make sure we re-compute utcnow() every time rows are extracted from a file.
        known_args.import_time = None

    return known_args, pipeline_args
