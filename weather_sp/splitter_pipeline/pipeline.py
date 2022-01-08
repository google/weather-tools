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

import argparse
import logging
import os
import typing as t

import apache_beam as beam
from apache_beam.io.fileio import MatchFiles, ReadMatches
import apache_beam.metrics as metrics
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from .file_name_utils import OutFileInfo, get_output_file_base_name
from .file_splitters import get_splitter

logger = logging.getLogger(__name__)


def configure_logger(verbosity: int) -> None:
    """Configures logging from verbosity. Default verbosity will show errors."""
    level = 40 - verbosity * 10
    logging.getLogger(__package__).setLevel(level)
    logger.setLevel(level)


def split_file(input_file: str, input_base_dir: str, output_template: str, dry_run: bool):
    logger.info('Splitting file %s', input_file)
    metrics.Metrics.counter('pipeline', 'splitting file').inc()
    splitter = get_splitter(input_file,
                            get_output_base_name(input_path=input_file,
                                                 input_base=input_base_dir,
                                                 output_template=output_template),
                            dry_run)
    splitter.split_data()


def _get_base_input_directory(input_pattern: str) -> str:
    base_dir = input_pattern
    for x in ['*', '?', '[']:
        base_dir = base_dir.split(x, maxsplit=1)[0]
    # Go one directory up to include the last common directory in output path.
    return os.path.dirname(os.path.dirname(base_dir))


def get_output_base_name(input_path: str, input_base: str,
                         output_template: str) -> OutFileInfo:
    return get_output_file_base_name(input_path, output_template, input_base)


def run(argv: t.List[str], save_main_session: bool = True):
    """Main entrypoint & pipeline definition."""
    parser = argparse.ArgumentParser(
        prog='weather-sp',
        description='Split weather data file into files by variable.'
    )
    parser.add_argument('-i', '--input-pattern', type=str, required=True,
                        help='Pattern for input weather data.')
    parser.add_argument('-o', '--output-template', type=str, required=True,
                        help='Template specifying path to output files. '
                             'Either a directory that will replace the common path of the '
                             'input_pattern, or a template using python-style formatting substitution'
                             'of input directory names.'
                             'For `input_pattern a/b/c/**` and file `a/b/c/file.nc`,'
                             '`output_template /x/y/z` will create'
                             'output files like `/x/y/z/c/file_shortname.nc`, while a template'
                             'with formating `/somewhere/{1}-{0}.` will give `somewhere/c-file.shortname.nc`'
                        )
    parser.add_argument('-d', '--dry-run', action='store_true', default=False,
                        help='Test the input file matching and the output file scheme without splitting.')
    known_args, pipeline_args = parser.parse_known_args(argv[1:])

    configure_logger(2)  # 0 = error, 1 = warn, 2 = info, 3 = debug

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    input_pattern = known_args.input_pattern
    input_base_dir = _get_base_input_directory(input_pattern)
    # output target directory, if empty, set to same as input
    output_template = known_args.output_template
    if not output_template:
        output_template = input_base_dir
    dry_run = known_args.dry_run

    logger.debug('input_pattern: %s', input_pattern)
    logger.debug('input_base_dir: %s', input_base_dir)
    logger.debug('output_template: %s', output_template)
    logger.debug('dry_run: %s', known_args.dry_run)
    with beam.Pipeline(options=pipeline_options) as p:
        (
                p
                | 'MatchFiles' >> MatchFiles(input_pattern)
                | 'ReadMatches' >> ReadMatches()
                | 'Shuffle' >> beam.Reshuffle()
                | 'GetPath' >> beam.Map(lambda x: x.metadata.path)
                | 'SplitFiles' >> beam.Map(split_file,
                                           input_base_dir,
                                           output_template,
                                           dry_run)
        )
