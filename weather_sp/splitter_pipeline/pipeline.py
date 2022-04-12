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

from .file_name_utils import OutFileInfo, get_output_file_info
from .file_splitters import get_splitter

logger = logging.getLogger(__name__)


def configure_logger(verbosity: int) -> None:
    """Configures logging from verbosity. Default verbosity will show errors."""
    level = 40 - verbosity * 10
    logging.getLogger(__package__).setLevel(level)
    logger.setLevel(level)


def split_file(input_file: str,
               input_base_dir: str,
               output_template: t.Optional[str],
               output_dir: t.Optional[str],
               formatting: str,
               dry_run: bool,
               force_split: bool = False):
    logger.info('Splitting file %s', input_file)
    metrics.Metrics.counter('pipeline', 'splitting file').inc()
    splitter = get_splitter(input_file,
                            get_output_base_name(input_path=input_file,
                                                 input_base=input_base_dir,
                                                 output_template=output_template,
                                                 output_dir=output_dir,
                                                 formatting=formatting),
                            dry_run,
                            force_split)
    splitter.split_data()


def _get_base_input_directory(input_pattern: str) -> str:
    base_dir = input_pattern
    for x in ['*', '?', '[']:
        base_dir = base_dir.split(x, maxsplit=1)[0]
    # Go one directory up to include the last common directory in output path.
    return os.path.dirname(os.path.dirname(base_dir))


def get_output_base_name(input_path: str,
                         input_base: str,
                         output_template: t.Optional[str],
                         output_dir: t.Optional[str],
                         formatting: str) -> OutFileInfo:
    return get_output_file_info(input_path,
                                input_base_dir=input_base,
                                out_pattern=output_template,
                                out_dir=output_dir,
                                formatting=formatting)


def run(argv: t.List[str], save_main_session: bool = True):
    """Main entrypoint & pipeline definition."""
    parser = argparse.ArgumentParser(
        prog='weather-sp',
        description='Split weather data file into files by variable.'
    )
    parser.add_argument('-i', '--input-pattern', type=str, required=True,
                        help='Pattern for input weather data.')
    output_options = parser.add_mutually_exclusive_group(required=True)
    output_options.add_argument(
        '--output-template', type=str,
        help='Template specifying path to output files using '
        'python-style formatting substitution of input '
        'directory names. '
        'For `input_pattern a/b/c/**` and file `a/b/c/file.grib`, '
        'a template with formatting `/somewhere/{1}-{0}.{level}_{shortName}.grib` '
        'will give `somewhere/c-file.level_shortName.grib`'
    )
    output_options.add_argument(
        '--output-dir', type=str,
        help='Output directory that will replace the common path of the '
        'input_pattern. '
        'For `input_pattern a/b/c/**` and file `a/b/c/file.nc`, '
        '`outputdir /x/y/z` will create '
        'output files like `/x/y/z/c/file_variable.nc`'
    )
    parser.add_argument(
        '--formatting', type=str, default='',
        help='Used in combination with `output-dir`: specifies the how to '
        'split the data and format the output file. '
        'Example: `_{time}_{level}hPa`'
    )
    parser.add_argument('-d', '--dry-run', action='store_true', default=False,
                        help='Test the input file matching and the output file scheme without splitting.')
    parser.add_argument('-f', '--force', action='store_true', default=False,
                        help='Force re-splitting of the pipeline. Turns of skipping of already split data.')
    known_args, pipeline_args = parser.parse_known_args(argv[1:])

    configure_logger(2)  # 0 = error, 1 = warn, 2 = info, 3 = debug

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    input_pattern = known_args.input_pattern
    input_base_dir = _get_base_input_directory(input_pattern)
    output_template = known_args.output_template
    output_dir = known_args.output_dir
    formatting = known_args.formatting

    if not output_template and not output_dir:
        raise ValueError('No output specified')
    dry_run = known_args.dry_run

    logger.debug('input_pattern: %s', input_pattern)
    logger.debug('input_base_dir: %s', input_base_dir)
    if output_template:
        logger.debug('output_template: %s', output_template)
    if output_dir:
        logger.debug('output_dir: %s', output_dir)
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
                                       output_dir,
                                       formatting,
                                       dry_run,
                                       known_args.force)
        )
