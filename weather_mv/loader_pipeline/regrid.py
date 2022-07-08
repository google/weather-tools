# Copyright 2022 Google LLC
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
import dataclasses
import json
import logging
import os.path
import shutil
import tempfile
import typing as t

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.gcsio import WRITE_CHUNK_SIZE

from .sinks import ToDataSink, open_local

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

try:
    import metview as mv
except (ModuleNotFoundError, ImportError, FileNotFoundError):
    logger.error('Metview could not be imported.')
    mv = None  # noqa


@dataclasses.dataclass
class Regrid(ToDataSink):
    output_path: str
    regrid_kwargs: t.Dict
    to_netcdf: bool = False

    @classmethod
    def add_parser_arguments(cls, subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument('-o', '--output_path', type=str, required=True,
                               help='The destination path for the regridded files.')
        subparser.add_argument('-k', '--regrid_kwargs', type=json.loads, default='{"grid": [0.25, 0.25]}',
                               help="""Keyword-args to pass into `metview.regrid()` in the form of a JSON string. """
                                    """Will default to '{"grid": [0.25, 0.25]}'.""")
        subparser.add_argument('--to_netcdf', action='store_true', default=False,
                               help='Write output file in NetCDF via XArray. Default: off')

    @classmethod
    def validate_arguments(cls, known_args: argparse.Namespace, pipeline_options: t.List[str]) -> None:
        pass

    def target_from(self, uri: str) -> str:
        """Create the target path from the input URI.

        Will change the extension if the target is a NetCDF.
        """
        base = os.path.basename(uri)
        in_dest = os.path.join(self.output_path, base)

        if not self.to_netcdf:
            return in_dest

        # If we convert to NetCDF, change the extension.
        no_ext, _ = os.path.splitext(in_dest)
        return f'{no_ext}.nc'

    def apply(self, uri: str):
        import glob

        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        file_regrid_label = f'{uri!r} -> {self.target_from(uri)!r}'
        logger.info(f'Regridding [{file_regrid_label!r}] ...')

        if self.dry_run:
            return

        with open_local(uri) as local_grib:
            # TODO(alxr): Figure out way to open fieldset in memory...
            logger.debug(f'Copied GRIB to local file [{file_regrid_label!r}].')
            try:
                fs = mv.bindings.Fieldset(path=local_grib)
                logger.debug(f'Read local GRIB to fieldset [{file_regrid_label!r}].')
                fieldset = mv.regrid(data=fs, **self.regrid_kwargs)
                logger.debug(f'Regridded the fieldset [{file_regrid_label!r}].')
            except (ModuleNotFoundError, ImportError, FileNotFoundError) as e:
                raise ImportError('Please install MetView with Anaconda:\n'
                                  '`conda install metview-batch -c conda-forge`') from e

        with tempfile.NamedTemporaryFile() as src:
            logger.debug(f'Writing regridded fieldset to local file [{file_regrid_label!r}].')
            if self.to_netcdf:
                fieldset.to_dataset().to_netcdf(src.name)
            else:
                mv.write(src.name, fieldset)
            logger.debug(f'Wrote regridded local file [{file_regrid_label!r}].')

            src.flush()

            logger.debug(f'Removing metview temporary directories [{file_regrid_label!r}] ...')
            cache_dir = glob.glob(f'{tempfile.gettempdir()}/mv*')[0]
            logger.debug(f'Located temporary directories to remove: {cache_dir} [{file_regrid_label!r}].')
            shutil.rmtree(cache_dir)
            logger.debug(f'Temporary directories removed [{file_regrid_label!r}].')
            os.makedirs(cache_dir)
            logger.debug(f'Re-building empty temporary directory structure [{file_regrid_label!r}].')

            logger.debug(f'Uploading regridded local file to final destination [{file_regrid_label!r}] ...')
            with FileSystems().create(self.target_from(uri)) as dst:
                logger.debug(f'Created final destination [{file_regrid_label!r}].')
                shutil.copyfileobj(src, dst, WRITE_CHUNK_SIZE)
                logger.debug(f'Upload to final destination complete [{file_regrid_label!r}].')
            logger.info(f'Finished regridding [{file_regrid_label!r}].')

    def expand(self, paths):
        paths | beam.Map(self.apply)
