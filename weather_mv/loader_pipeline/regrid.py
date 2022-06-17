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
import metview as mv
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.gcsio import WRITE_CHUNK_SIZE

from .sinks import ToDataSink, open_local

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclasses.dataclass
class Regrid(ToDataSink):
    output_path: str
    regrid_kwargs: t.Dict
    to_netcdf: bool = False

    @classmethod
    def add_parser_arguments(cls, subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument('--output_path', type=str, required=True,
                               help='The destination path for the regridded files.')
        subparser.add_argument('--regrid_kwargs', type=json.loads, default='{"grid": [0.25, 0.25]}',
                               help="""Keyword-args to pass into `metview.regrid()` in the form of a JSON string. """
                                    """Will default to '{"grid": [0.25, 0.25]}'.""")
        subparser.add_argument('--to_netcdf', action='store_true', default=False,
                               help='Write output file in NetCDF via XArray. Default: off')

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
        logger.info(f'Regridding from {uri!r} to {self.target_from(uri)!r}.')

        if self.dry_run:
            return

        with open_local(uri) as local_grib:
            # TODO(alxr): Figure out way to open fieldset in memory...
            fieldset = mv.read(source=local_grib, **self.regrid_kwargs)

        with tempfile.NamedTemporaryFile() as src:
            if self.to_netcdf:
                fieldset.to_dataset().to_netcdf(src.name)
            else:
                mv.write(src.name, fieldset)

            src.flush()

            with FileSystems().create(self.target_from(uri)) as dst:
                shutil.copyfileobj(src, dst, WRITE_CHUNK_SIZE)

    def expand(self, paths):
        paths | beam.Map(self.apply)
