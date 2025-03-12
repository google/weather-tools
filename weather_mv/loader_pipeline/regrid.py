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
import contextlib
import dataclasses
import glob
import json
import logging
import os.path
import shutil
import subprocess
import tempfile
import typing as t
import warnings

import apache_beam as beam
import dask
import xarray as xr
import xarray_beam as xbeam

from .sinks import ToDataSink, open_local, copy, path_exists

logger = logging.getLogger(__name__)

try:
    import metview as mv
    Fieldset = mv.bindings.Fieldset
except (ModuleNotFoundError, ImportError, FileNotFoundError, ValueError):
    logger.error('Metview could not be imported.')
    mv = None  # noqa
    Fieldset = t.Any


def _clear_metview():
    """Clear the metview temporary directory.

    By default, caches are cleared when the MetView _process_ ends.
    This method is necessary to free space sooner than that, namely
    after invoking MetView functions.
    """
    cache_dirs = glob.glob(f'{tempfile.gettempdir()}/mv.*')
    for cache_dir in cache_dirs:
        shutil.rmtree(cache_dir)
        os.makedirs(cache_dir)


@contextlib.contextmanager
def _metview_op() -> t.Iterator[None]:
    """Perform operation with MetView, including error handling and cleanup."""
    try:
        yield
    except (ModuleNotFoundError, ImportError, FileNotFoundError) as e:
        raise ImportError('Please install MetView with Anaconda:\n'
                          '`conda install metview-batch -c conda-forge`') from e
    finally:
        _clear_metview()


class MapChunkAsFieldset(beam.PTransform):
    """Apply an operation with MetView on a xarray.Dataset as if it's a metview.Fieldset.

    This transform will handle converting to and from xr.Datasets to mv.Fieldsets. This
    allows the user to perform any MetView or Fieldset operation within the overridable
    `apply()` method.

    > Warning: This cannot process large Datasets without a decent amount of disk space!
    """

    def apply(self, key: xbeam.Key, fs: Fieldset) -> t.Tuple[xbeam.Key, Fieldset]:
        return key, fs

    def _apply(self, key: xbeam.Key, ds: xr.Dataset) -> t.Tuple[xbeam.Key, xr.Dataset]:
        # Clear metadata so ecCodes doesn't mess up the conversion. Instead of default grib fields,
        # ecCodes will use the parameter ID. Thus, the fields will appear in the final, regridded
        # dataset.
        for dv in ds.data_vars:
            for to_del in ['GRIB_cfName', 'GRIB_shortName', 'GRIB_cfVarName']:
                if to_del in ds[dv].attrs:
                    del ds[dv].attrs[to_del]

        with _metview_op():
            # mv.dataset_to_fieldset() will error on input where there is only 1 value
            # in a dimension. ECMWF's cfgrib is in its alpha version.
            try:
                fs = mv.dataset_to_fieldset(ds)
            except ValueError as e:
                raise ValueError(
                    'please change `zarr_input_chunk`s so that there are no'
                    'single element dimensions (e.g. {"time": 1} is not allowed).'
                ) from e

            # Apply any & all MetView or FieldSet operations.
            kout, fs_out = self.apply(key, fs)

            return kout, fs_out.to_dataset().compute()

    def expand(self, pcoll):
        return pcoll | beam.MapTuple(self._apply)


@dataclasses.dataclass
class RegridChunk(MapChunkAsFieldset):
    """Regrid a xarray.Dataset with MetView.

    Attributes:
        regrid_kwargs: A dictionary of keyword-args to be passed into `mv.regrid()`
            (excluding the dataset).
        zarr_input_chunks: (Optional) When regridding Zarr data, how the input
            dataset should be chunked upon open.
    """
    regrid_kwargs: t.Dict
    zarr_input_chunks: t.Optional[t.Dict] = None

    def template(self, source_ds: xr.Dataset) -> xr.Dataset:
        """Calculate the output Zarr template by regridding (a tiny slice of) the input dataset."""
        # Silence Dask warning...
        with dask.config.set(**{'array.slicing.split_large_chunks': False}):
            zeros = source_ds.chunk().pipe(xr.zeros_like)

            # If the chunked source dataset is small (less than 10 MB), just regrid it!
            if (zeros.nbytes / 1024 / 1024) < 10:
                _, ds = self._apply(xbeam.Key(), zeros)
                return ds.chunk()

            # source_ds is probably very big! Let's shrink it by a non-spatial dimension
            # so calculating the template will be tractable...

            # Get a single timeslice of the zeros Dataset (or equivalent chunkable dimension).
            # We don't know for sure that 'time' is in the Zarr dataset, so here we make our
            # best attempt to find a good slice.
            t0 = None
            for dim in ['time', *(self.zarr_input_chunks or {}).keys()]:
                if dim in zeros:
                    t0 = zeros.isel({dim: 0}, drop=True)
                    break
            if t0 is None:
                raise ValueError('cannot infer any dimension when creating a Zarr template. '
                                 'Please define at least one chunk in `--zarr_input_chunks`.')

            _, ds = self._apply(xbeam.Key(), t0)
            # Regrid the single time, then expand the Dataset to span all times.
            tmpl = (
                ds
                .chunk()
                .expand_dims({dim: zeros[dim]}, 0)
            )

            return tmpl

    def apply(self, key: xbeam.Key, fs: Fieldset) -> t.Tuple[xbeam.Key, Fieldset]:
        return key, mv.regrid(data=fs, **self.regrid_kwargs)


@dataclasses.dataclass
class Regrid(ToDataSink):
    """Regrid data using MetView.

    See https://metview.readthedocs.io/en/latest/metview/using_metview/regrid_intro.html
    for an in-depth intro on regridding with MetView.

    Attributes:
        output_path: URI for regridding target. Can be a glob pattern of NetCDF or Grib files; optionally,
            it can be a Zarr corpus is supported.
        regrid_kwargs: A dictionary of keyword-args to be passed into `mv.regrid()` (excluding the dataset).
        to_netcdf: When set, it raw data output will be written as NetCDF. Cannot use with Zarr datasets.
        zarr_input_chunks: (Optional) When regridding Zarr data, how the input dataset should be chunked upon open.
        zarr_output_chunks: (Optional, recommended) When regridding Zarr data, how the output Zarr dataset should be
            divided into chunks.
    """
    output_path: str
    regrid_kwargs: t.Dict
    force_regrid: bool = False
    to_netcdf: bool = False
    zarr_input_chunks: t.Optional[t.Dict] = None
    zarr_output_chunks: t.Optional[t.Dict] = None

    @classmethod
    def add_parser_arguments(cls, subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument('-o', '--output_path', type=str, required=True,
                               help='The destination path for the regridded files.')
        subparser.add_argument('-k', '--regrid_kwargs', type=json.loads, default='{"grid": [0.25, 0.25]}',
                               help="""Keyword-args to pass into `metview.regrid()` in the form of a JSON string. """
                                    """Will default to '{"grid": [0.25, 0.25]}'.""")
        subparser.add_argument('--force_regrid', action='store_true', default=False,
                               help='Force regrid all files even if file is present at output_path.')
        subparser.add_argument('--to_netcdf', action='store_true', default=False,
                               help='Write output file in NetCDF via XArray. Default: off')
        subparser.add_argument('-zi', '--zarr_input_chunks', type=json.loads, default=None,
                               help='When reading a Zarr, break up the data into chunks. Takes a JSON string.')
        subparser.add_argument('-zo', '--zarr_output_chunks', type=json.loads, default=None,
                               help='When writing a Zarr, write the data with chunks. Takes a JSON string.')

    @classmethod
    def validate_arguments(cls, known_args: argparse.Namespace, pipeline_options: t.List[str]) -> None:
        if known_args.zarr and known_args.to_netcdf:
            raise ValueError('only Zarr-to-Zarr regridding is allowed!')

        if not known_args.zarr and (known_args.zarr_input_chunks or known_args.zarr_output_chunks):
            raise ValueError('chunks can only be set when input URI is a Zarr.')

        if known_args.zarr:
            # Encourage use of correct output_path format.
            _, out_ext = os.path.splitext(known_args.output_path)
            if out_ext not in ['', '.zarr']:
                warnings.warn('if input is a Zarr, the output_path must also be a Zarr.', RuntimeWarning)

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

    def is_grib_file_corrupt(self, local_grib: str) -> bool:
        try:
            # Run grib_ls command to check the file
            subprocess.check_output(['grib_ls', local_grib])
            return False
        except subprocess.CalledProcessError as e:
            logger.info(f"Encountered error while reading GRIB: {e}.")
            return True

    def apply(self, uri: str) -> None:
        logger.info(f'Regridding from {uri!r} to {self.target_from(uri)!r}.')

        if self.dry_run:
            return

        if path_exists(self.target_from(uri), self.force_regrid):
            logger.info(f"Skipping {uri}.")
            return

        with _metview_op():
            try:
                logger.info(f'Copying grib from {uri!r} to local disk.')

                with open_local(uri) as local_grib:
                    logger.info(f"Checking for {uri}'s validity...")
                    if self.is_grib_file_corrupt(local_grib):
                        logger.error(f"Corrupt GRIB file found: {uri}.")
                        return
                    logger.info(f"No issues found with {uri}.")

                    logger.info(f'Regridding {uri!r}.')
                    fs = mv.bindings.Fieldset(path=local_grib)
                    fieldset = mv.regrid(data=fs, **self.regrid_kwargs)

                with tempfile.NamedTemporaryFile() as src:
                    logger.info(f'Writing {self.target_from(uri)!r} to local disk.')
                    if self.to_netcdf:
                        fieldset.to_dataset().to_netcdf(src.name)
                    else:
                        mv.write(src.name, fieldset)

                    src.flush()

                    _clear_metview()

                    logger.info(f'Uploading {self.target_from(uri)!r}.')
                    copy(src.name, self.target_from(uri))
            except Exception as e:
                logger.info(f'Regrid failed for {uri!r}. Error: {str(e)}')

    def expand(self, paths):
        if not self.zarr:
            paths | beam.Map(self.apply)
            return

        # Since `chunks=None` here, data will be opened lazily upon access.
        # This is used to get the Zarr metadata without loading the data.
        source_ds = xr.open_zarr(self.first_uri, **self.zarr_kwargs)

        regrid_op = RegridChunk(self.regrid_kwargs, self.zarr_input_chunks)

        regridded = (
                paths
                | xbeam.DatasetToChunks(source_ds, self.zarr_input_chunks)
                | 'RegridChunk' >> regrid_op
        )

        tmpl = paths | beam.Create([source_ds]) | 'CalcZarrTemplate' >> beam.Map(regrid_op.template)

        to_write = regridded
        if self.zarr_output_chunks:
            to_write |= xbeam.ConsolidateChunks(self.zarr_output_chunks)

        to_write | xbeam.ChunksToZarr(self.output_path, beam.pvalue.AsSingleton(tmpl), self.zarr_output_chunks)
