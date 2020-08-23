"""Primary ECMWF Downloader Workflow."""

import argparse
import datetime
import typing as t

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from ecmwf_pipeline.parsers import date

TODAY = datetime.date.today()


def run(argv: t.List[str], save_main_session: bool = True):
    """Main entrypoint & pipeline definition."""
    parser = argparse.ArgumentParser()
    parser.add_argument('config', type=argparse.FileType('r', encoding='utf-8'),
                        help='path/to/config.cfg, specific to the <client>. Accepts *.cfg and *.json files.')
    parser.add_argument('start', type=date,
                        help="Start of date-range (inclusive), in 'YYYY-MM-DD' format.")
    parser.add_argument('end', type=date,
                        help="End of date-range (inclusive), in 'YYYY-MM-DD' format.")
    parser.add_argument('-c', '--client', type=str, choices=['cdn'], default='cdn',
                        help="Choose a weather API client; default is 'cnd'.")

    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        # TODO(AWG): Implement
        pass

