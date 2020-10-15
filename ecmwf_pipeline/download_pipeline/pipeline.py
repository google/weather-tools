"""Primary ECMWF Downloader Workflow."""

import argparse
import itertools
import logging
import tempfile
import typing as t

import apache_beam as beam
import apache_beam.metrics
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp import gcsio

from .clients import CLIENTS, Client
from .parsers import process_config


def prepare_partition(config: t.Dict) -> t.Iterator[t.Dict]:
    """Iterate over client parameters, partitioning over `partition_keys`."""
    partition_keys = config['parameters']['partition_keys']
    selection = config.get('selection', {})

    # Produce a Cartesian-Cross over the range of keys.
    # For example, if the keys were 'year' and 'month', it would produce
    # an iterable like: ( ('2020', '01'), ('2020', '02'), ('2020', '03'), ...)
    fan_out = itertools.product(*[selection[key] for key in partition_keys])

    # Output a config dictionary, overriding the range of values for
    # each key with the partition instance in 'selection'.
    # Continuing the example:
    #   { 'foo': ..., 'year': ['2020'], 'month': ['01'], ... }
    #   { 'foo': ..., 'year': ['2020'], 'month': ['02'], ... }
    #   { 'foo': ..., 'year': ['2020'], 'month': ['03'], ... }
    for option in fan_out:
        copy = selection.copy()
        out = config.copy()
        for idx, key in enumerate(partition_keys):
            copy[key] = [option[idx]]
        out['selection'] = copy
        yield out


def fetch_data(config: t.Dict, *, client: Client) -> None:
    """
    Download data from a client to a temp file, then upload to Google Cloud Storage.
    """
    dataset = config['parameters'].get('dataset', '')

    partition_keys = config['parameters']['partition_keys']
    partition_key_values = [config['selection'][key][0] for key in partition_keys]
    target = config['parameters']['target_template'].format(*partition_key_values)

    selection = config['selection']

    with tempfile.NamedTemporaryFile() as temp:
        try:
            logging.info(f'Fetching data for {target}')
            client.retrieve(dataset, selection, temp.name)

            # upload blob to gcs
            logging.info(f'Uploading to GCS for {target}')
            temp.seek(0)
            with gcsio.GcsIO().open(target, 'wb') as dest:
                while True:
                    chunk = temp.read(8192)
                    if len(chunk) == 0:  # eof
                        break
                    dest.write(chunk)
            logging.info(f'Upload to GCS complete for {target}')
            beam.metrics.Metrics.counter('Success', 'FetchData').inc()

        except Exception as e:
            logging.error(f'Unable to retrieve/store data for {target}: {e}')
            beam.metrics.Metrics.counter('Failure', 'FetchData').inc()


def run(argv: t.List[str], save_main_session: bool = True):
    """Main entrypoint & pipeline definition."""
    parser = argparse.ArgumentParser()
    parser.add_argument('config', type=argparse.FileType('r', encoding='utf-8'),
                        help='path/to/config.cfg, specific to the <client>. Accepts *.cfg and *.json files.')
    parser.add_argument('-c', '--client', type=str, choices=CLIENTS.keys(), default=next(iter(CLIENTS.keys())),
                        help=f"Choose a weather API client; default is '{next(iter(CLIENTS.keys()))}'.")

    known_args, pipeline_args = parser.parse_known_args(argv[1:])

    config = {}
    with known_args.config as f:
        config = process_config(f)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    client = CLIENTS[known_args.client](config)

    with beam.Pipeline(options=pipeline_options) as p:
        (
                p
                | 'Create' >> beam.Create(prepare_partition(config))
                | 'FetchData' >> beam.Map(fetch_data, client=client)
        )
