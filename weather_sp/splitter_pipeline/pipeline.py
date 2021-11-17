import argparse
import logging
import typing as t

import apache_beam as beam
from apache_beam.io.fileio import MatchFiles, ReadMatches
import apache_beam.metrics as metrics
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from .file_splitters import get_splitter, SPLIT_DIRECTORY


def configure_logger(verbosity: int) -> None:
    """Configures logging from verbosity. Default verbosity will show errors."""
    logging.basicConfig(level=(40 - verbosity * 10),
                        format='%(asctime)-15s %(message)s')


def split_file(path: str):
    if SPLIT_DIRECTORY in path:
        metrics.Metrics.counter('pipeline', 'file skipped, already split').inc()
        logging.info('Skipping already split file %s', path)
        return
    logging.info('Splitting file %s', path)
    metrics.Metrics.counter('pipeline', 'splitting file').inc()
    splitter = get_splitter(path)
    splitter.split_data()


def run(argv: t.List[str], save_main_session: bool = True):
    """Main entrypoint & pipeline definition."""
    parser = argparse.ArgumentParser(
        prog='weather-splitter',
        description='Split weather data file into files by variable.'
    )
    parser.add_argument('-i', '--input_pattern', type=str, required=True,
                        help='Pattern for input weather data.')
    known_args, pipeline_args = parser.parse_known_args(argv[1:])

    configure_logger(3)  # 0 = error, 1 = warn, 2 = info, 3 = debug

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | 'MatchFiles' >> MatchFiles(known_args.input_pattern)
            | 'ReadMatches' >> ReadMatches()
            | 'Shuffle' >> beam.Reshuffle()
            | 'GetPath' >> beam.Map(lambda x: x.metadata.path)
            | 'SplitFiles' >> beam.Map(split_file)
        )
