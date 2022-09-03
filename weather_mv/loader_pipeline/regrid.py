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
import glob
import json
import logging
import os.path
import shutil
import tempfile
import typing as t

import apache_beam as beam
import apache_beam.pvalue
import xarray_beam as xbeam
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.gcsio import WRITE_CHUNK_SIZE

from .sinks import ToDataSink, open_local

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
        if known_args.zarr and known_args.to_netcdf:
            raise ValueError('only Zarr-to-Zarr regridding is allowed!')

    def target_from(self, uri: str) -> str:
        """Create the target path from the input URI.

        In the case of Zarr, the output will be treated like a valid path.
        For NetCDF, this will change the extension to '.nc'.
        """
        if self.zarr:
            return self.output_path

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

        logger.debug(f'Copying grib from {uri!r} to local disk.')
        with open_local(uri) as local_grib:
            # TODO(alxr): Figure out way to open fieldset in memory...
            logger.debug(f'Regridding {uri!r}.')
            try:
                fs = mv.bindings.Fieldset(path=local_grib)
                fieldset = mv.regrid(data=fs, **self.regrid_kwargs)
            except (ModuleNotFoundError, ImportError, FileNotFoundError) as e:
                raise ImportError('Please install MetView with Anaconda:\n'
                                  '`conda install metview-batch -c conda-forge`') from e

        with tempfile.NamedTemporaryFile() as src:
            logger.debug(f'Writing {self.target_from(uri)!r} to local disk.')
            if self.to_netcdf:
                fieldset.to_dataset().to_netcdf(src.name)
            else:
                mv.write(src.name, fieldset)

            src.flush()

            # Clear the metview temporary directory.
            cache_dir = glob.glob(f'{tempfile.gettempdir()}/mv*')[0]
            shutil.rmtree(cache_dir)
            os.makedirs(cache_dir)

            logger.info(f'Uploading {self.target_from(uri)!r}.')
            with FileSystems().create(self.target_from(uri)) as dst:
                shutil.copyfileobj(src, dst, WRITE_CHUNK_SIZE)

    def expand(self, paths):
        if not self.zarr:
            paths | beam.Map(self.apply)
            return

        # TODO(alxr): Create xbeam pipeline for processing Zarr.
        # WIP:
        # source_zarr = apache_beam.pvalue.AsSingleton(paths)
        # xbeam.DatasetToChunks()
