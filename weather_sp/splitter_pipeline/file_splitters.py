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

import abc
import itertools
import logging
import os
import re
import shutil
import string
import subprocess
import tempfile
import typing as t
import metview as mv
from contextlib import contextmanager

import apache_beam.metrics as metrics
import numpy as np
import pygrib
import xarray as xr
from apache_beam.io.filesystems import FileSystems

from .file_name_utils import OutFileInfo

logger = logging.getLogger(__name__)


# TODO(#245): Group with common utilities (duplicated)
def copy(src: str, dst: str) -> None:
    """Copy data via `gsutil cp`."""
    try:
        subprocess.run(['gsutil', 'cp', src, dst], check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        logger.error(f'Failed to copy file {src!r} to {dst!r} due to {e.stderr.decode("utf-8")}')
        raise


class FileSplitter(abc.ABC):
    """Base class for weather file splitters."""

    def __init__(self, input_path: str, output_info: OutFileInfo,
                 force_split: bool = False, logging_level: int = logging.INFO, grib_filter_expression: str = None):
        self.input_path = input_path
        self.output_info = output_info
        self.force_split = force_split
        self.logger = logging.getLogger(f'{__name__}.{type(self).__name__}')
        self.logger.setLevel(logging_level)
        self.logger.debug('Splitter for path=%s, output base=%s',
                          self.input_path, self.output_info)
        self.grib_filter_expression = grib_filter_expression

    @abc.abstractmethod
    def split_data(self) -> None:
        raise NotImplementedError()

    @contextmanager
    def _copy_to_local_file(self) -> t.Iterator[t.IO]:
        self.logger.info(f'Copying {self.input_path!r} locally.')
        with tempfile.NamedTemporaryFile() as dest_file:
            copy(self.input_path, dest_file.name)
            yield dest_file

    def should_skip(self):
        """Skip splitting if the data was already split."""
        if self.force_split:
            return False

        for match in FileSystems().match([
            self.output_info.formatted_output_path(
                {var: '*' for var in self.output_info.split_dims()}),
        ]):
            if len(match.metadata_list) > 0:
                return True
        return False

    def should_skip_file(self, output_path: str) -> bool:
        """Skip splitting if the data file was already split.

        TODO(#287): Consider making this the default skipping implementation...
        """
        if self.force_split:
            return False

        matches = FileSystems().match([output_path])
        assert len(matches) == 1
        if len(matches[0].metadata_list) > 0:
            return True
        return False


class GribSplitter(FileSplitter):

    def split_data(self) -> None:
        if not self.output_info.split_dims():
            raise ValueError('No splitting specified in template.')

        if self.should_skip():
            metrics.Metrics.counter('file_splitters', 'skipped').inc()
            self.logger.info('Skipping %s, file already split.',
                             repr(self.input_path))
            return

        # Here, we keep a map of open file objects (`outputs`). We need these since
        # each output grib file (named `key`) will include multiple `grb` messages
        # each. By writing data to the cache of open file objects, we can keep a
        # minimal amount of data in memory at a time.
        outputs = dict()
        with self._open_grib_locally() as grbs:
            self.logger.info('Splitting & uploading %r...', self.input_path)
            try:
                for grb in grbs:
                    # Iterate through the split dimensions of the grib message in order to
                    # produce the right output file.
                    splits = dict()
                    for dim in self.output_info.split_dims():
                        try:
                            splits[dim] = getattr(grb, dim)
                        except RuntimeError:
                            self.logger.error(
                                'Variable not found in grib: %s', dim)
                    key = self.output_info.formatted_output_path(splits)

                    # Append the current grib message to a set number of output files.
                    # If the target shard doesn't exist, create it.
                    if key not in outputs:
                        outputs[key] = FileSystems.create(key)
                    outputs[key].write(grb.tostring())
                    outputs[key].flush()

                    # Delete the grib message from memory – *and disk* – before moving on to the next
                    # grib message. See the pygrib sources for more info.
                    # https://github.com/jswhit/pygrib/blob/v2.1.4rel/src/pygrib/_pygrib.pyx#L759
                    del grb
            finally:
                for out in outputs.values():
                    out.close()
            self.logger.info('Split %s into %d files',
                             self.input_path, len(outputs))

    @contextmanager
    def _open_grib_locally(self) -> t.Iterator[t.Iterator[pygrib.gribmessage]]:
        with self._copy_to_local_file() as local_file:
            with pygrib.open(local_file.name) as gb:
                yield gb


class GribSplitterV2(GribSplitter):
    """Splitter that makes use of `grib_copy` util for high performance splitting.

    See https://confluence.ecmwf.int/display/ECC/grib_copy.
    """

    def replace_non_numeric_bracket(self, match: re.Match) -> str:
        value = match.group(1)
        return f"[{value}]" if not value.isdigit() else "{" + value + "}"

    def split_data(self) -> None:
        if not self.output_info.split_dims():
            raise ValueError('No splitting specified in template.')

        grib_copy_cmd = shutil.which('grib_copy')
        grib_get_cmd = shutil.which('grib_get')
        uniq_cmd = shutil.which('uniq')
        for cmd, name in [(grib_get_cmd, 'grib_copy'), (grib_get_cmd, 'grib_get'), (uniq_cmd, 'uniq')]:
            if not cmd:
                raise EnvironmentError(f'binary {name!r} is not available in the current environment!')

        unformatted_output_path = self.output_info.unformatted_output_path()
        prefix, _ = os.path.split(next(iter(string.Formatter().parse(unformatted_output_path)))[0])
        _, tail = unformatted_output_path.split(prefix)

        # Replace { with [ and } with ] only for non-numeric values inside {} of tail
        output_str = re.sub(r'\{(\w+)\}', self.replace_non_numeric_bracket, tail)
        output_template = output_str.format(*self.output_info.template_folders)

        slash = '/'
        delimiter = 'DELIMITER'
        flat_output_template = output_template.replace('/', delimiter)
        split_dims = self.output_info.split_dims()
        with self._copy_to_local_file() as local_file:
            self.logger.info('Skipping as needed...')
            grib_fields = mv.read(local_file.name)
            split_values = mv.grib_get(grib_fields, split_dims)
            output_paths = []
            skipped_paths = []
            for line in split_values:
                splits = dict(zip(split_dims, line))
                output_path = self.output_info.formatted_output_path(splits)
                if self.should_skip_file(output_path):
                    skipped_paths.append(output_path)
                    continue
                output_paths.append(output_path)
            if not output_paths:
                metrics.Metrics.counter('file_splitters', 'skipped').inc()
                self.logger.info('Skipping %s, file already split into: %s',
                                 repr(self.input_path), ', '.join(skipped_paths))
                return

            with tempfile.TemporaryDirectory() as tmpdir:
                self.logger.info('Performing split.')
                dest = os.path.join(tmpdir, flat_output_template)
                if self.grib_filter_expression:
                    subprocess.run([grib_copy_cmd, "-w",
                                    self.grib_filter_expression,
                                    local_file.name, dest], check=True)
                else:
                    subprocess.run([grib_copy_cmd, local_file.name, dest],
                                   check=True)

                self.logger.info('Uploading %r...', self.input_path)
                for flat_target in os.listdir(tmpdir):
                    dest_file_path = f'{prefix}{flat_target.replace(delimiter, slash)}'
                    self.logger.info([prefix, dest_file_path, local_file.name,
                                      self.output_info.unformatted_output_path()])

                    copy(os.path.join(tmpdir, flat_target), dest_file_path)
                self.logger.info('Finished uploading %r', self.input_path)


class NetCdfSplitter(FileSplitter):

    _UNSUPPORTED_DIMENSIONS = ('latitude', 'longitude', 'lat', 'lon')

    def split_data(self) -> None:
        if not self.output_info.split_dims():
            raise ValueError('No splitting specified in template.')
        if any(dim in self._UNSUPPORTED_DIMENSIONS for dim in self.output_info.split_dims()):
            raise ValueError('Unsupported split dimension (lat, lng).')
        if self.should_skip():
            metrics.Metrics.counter('file_splitters', 'skipped').inc()
            self.logger.info('Skipping %s, file already split.',
                             repr(self.input_path))
            return

        with self._open_dataset_locally() as dataset:
            if any(split not in dataset.dims and split not in ('variable') for split in self.output_info.split_dims()):
                raise ValueError(
                    'netcdf split: requested dimension not in dataset')
            iterlists = []
            if 'variable' in self.output_info.split_dims():
                iterlists.append([dataset[var].to_dataset()
                                  for var in dataset.data_vars])
            else:
                iterlists.append([dataset])
            filtered_split_dims = [
                x for x in self.output_info.split_dims() if x not in ('variable', self._UNSUPPORTED_DIMENSIONS)]
            for dim in filtered_split_dims:
                iterlists.append(dataset[dim])
            combinations = itertools.product(*iterlists)
            self.logger.info('Splitting & uploading %r...', self.input_path)
            for comb in combinations:
                selected = comb[0]
                for da in comb[1:]:
                    for dim in da.coords:
                        selected = selected.sel({dim: getattr(da, dim)})
                self._write_dataset(selected, filtered_split_dims)
            self.logger.info('Finished splitting & uploading %r.', self.input_path)

    @contextmanager
    def _open_dataset_locally(self) -> t.Iterator[xr.Dataset]:
        with self._copy_to_local_file() as local_file:
            ds = xr.open_dataset(local_file.name, engine='netcdf4')
            yield ds
            ds.close()

    def _write_dataset(self, dataset: xr.Dataset, split_dims: t.List[str]) -> None:
        """Write destination NetCDF file in NETCDF4 format."""
        # Here, we need to write the file locally, since only the scipy engine supports file objects or
        # returning bytes. Further, the scipy engine does not support NETCDF4 (which is HDF5 compliant).
        # Storing data in HDF5 is advantageous since it allows opening NetCDF files with buffered readers.
        with tempfile.NamedTemporaryFile() as tmp:
            dataset.to_netcdf(path=tmp.name, engine='netcdf4', format='NETCDF4')
            copy(tmp.name, self._get_output_for_dataset(dataset, split_dims))

    def _get_output_for_dataset(self, dataset: xr.Dataset, split_dims: t.List[str]) -> str:
        splits = {'variable': list(dataset.data_vars.keys())[0]}
        for dim in split_dims:
            value = dataset[dim].values
            if dim == 'time':
                value = np.datetime_as_string(value, unit='m')
            splits[dim] = value
        return self.output_info.formatted_output_path(splits)


class DrySplitter(FileSplitter):

    def split_data(self) -> None:
        if not self.output_info.split_dims():
            raise ValueError('No splitting specified in template.')
        self.logger.info('input file: %s - output scheme: %s',
                         self.input_path, self.output_info.formatted_output_path(self._get_keys()))

    def _get_keys(self) -> t.Dict[str, str]:
        return {name: name for name in self.output_info.split_dims()}


def get_splitter(file_path: str,
                 output_info: OutFileInfo,
                 dry_run: bool,
                 force_split: bool = False,
                 logging_level: int = logging.INFO,
                 grib_filter_expression: t.Optional[str] = None) -> FileSplitter:
    if dry_run:
        logger.info('Using splitter: DrySplitter')
        return DrySplitter(file_path, output_info, logging_level=logging_level)

    with FileSystems.open(file_path) as f:
        header = f.read(4)

    if b'GRIB' in header:
        metrics.Metrics.counter('get_splitter', 'grib').inc()

        # Decide which version of the grib splitter to use depending on if ecCodes is installed.
        # Prefer the v2 grib splitter, which should be more robust -- especially when splitting by
        # multiple dimensions at once.
        cmd = shutil.which('grib_copy')
        if cmd:
            logger.info('Using splitter: GribSplitterV2')
            return GribSplitterV2(file_path, output_info, force_split,
                                  logging_level, grib_filter_expression)
        else:
            logger.info('Using splitter: GribSplitter')
            return GribSplitter(file_path, output_info, force_split,
                                logging_level)

    # See the NetCDF Spec docs:
    # https://docs.unidata.ucar.edu/netcdf-c/current/faq.html#How-can-I-tell-which-format-a-netCDF-file-uses
    if b'CDF' in header or b'HDF' in header:
        metrics.Metrics.counter('get_splitter', 'netcdf').inc()
        logger.info('Using splitter: NetCdfSplitter')
        return NetCdfSplitter(file_path, output_info, force_split)

    raise ValueError(
        f'cannot determine if file {file_path!r} is Grib or NetCDF.')
