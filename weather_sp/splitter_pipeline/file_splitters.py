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
import numpy as np
import pygrib
import shutil
import tempfile
import typing as t
import xarray as xr
from contextlib import contextmanager

import apache_beam.metrics as metrics
from apache_beam.io.filesystems import FileSystems

from .file_name_utils import OutFileInfo

logger = logging.getLogger(__name__)


class FileSplitter(abc.ABC):
    """Base class for weather file splitters."""

    def __init__(self, input_path: str, output_info: OutFileInfo,
                 force_split: bool = False, logging_level: int = logging.INFO):
        self.input_path = input_path
        self.output_info = output_info
        self.force_split = force_split
        self.logger = logging.getLogger(f'{__name__}.{type(self).__name__}')
        self.logger.setLevel(logging_level)
        self.logger.debug('Splitter for path=%s, output base=%s',
                          self.input_path, self.output_info)

    @abc.abstractmethod
    def split_data(self) -> None:
        raise NotImplementedError()

    @contextmanager
    def _copy_to_local_file(self) -> t.Iterator[t.IO]:
        with FileSystems().open(self.input_path) as source_file:
            with tempfile.NamedTemporaryFile() as dest_file:
                shutil.copyfileobj(source_file, dest_file)
                dest_file.flush()
                yield dest_file

    def _copy_dataset_to_storage(self, src_file: t.IO, target: str):
        with FileSystems().create(target) as dest_file:
            shutil.copyfileobj(src_file, dest_file)

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


class GribSplitter(FileSplitter):

    def __init__(self, input_path: str, output_info: OutFileInfo,
                 force_split: bool = False, logging_level: int = logging.INFO):
        super().__init__(input_path, output_info,
                         force_split, logging_level)

    def split_data(self) -> None:
        if not self.output_info.split_dims():
            raise ValueError('No splitting specified in template.')
        outputs = dict()

        if self.should_skip():
            metrics.Metrics.counter('file_splitters', 'skipped').inc()
            self.logger.info('Skipping %s, file already split.',
                             repr(self.input_path))
            return

        with self._open_grib_locally() as grbs:
            for grb in grbs:
                splits = dict()
                for dim in self.output_info.split_dims():
                    try:
                        splits[dim] = getattr(grb, dim)
                    except RuntimeError:
                        self.logger.error(
                            'Variable not found in grib: %s', dim)
                key = self.output_info.formatted_output_path(splits)
                if key not in outputs:
                    outputs[key] = FileSystems.create(key)
                outputs[key].write(grb.tostring())
                outputs[key].flush()

            for out in outputs.values():
                out.close()
            self.logger.info('split %s into %d files',
                             self.input_path, len(outputs))

    @contextmanager
    def _open_grib_locally(self) -> t.Iterator[t.Iterator[pygrib.gribmessage]]:
        with self._copy_to_local_file() as local_file:
            yield pygrib.open(local_file.name)


class NetCdfSplitter(FileSplitter):

    _UNSUPPORTED_DIMENSIONS = ('latitude', 'longitude', 'lat', 'lon')

    def __init__(self, input_path: str, output_info: OutFileInfo,
                 force_split: bool = False, logging_level: int = logging.INFO):
        super().__init__(input_path, output_info,
                         force_split, logging_level)

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
            for comb in combinations:
                selected = comb[0]
                for da in comb[1:]:
                    for dim in da.coords:
                        selected = selected.sel({dim: getattr(da, dim)})
                self._write_dataset(selected, filtered_split_dims)
            self.logger.info('Finished splitting %s', self.input_path)

    @contextmanager
    def _open_dataset_locally(self) -> t.Iterator[xr.Dataset]:
        with self._copy_to_local_file() as local_file:
            yield xr.open_dataset(local_file.name, engine='netcdf4')

    def _write_dataset(self, dataset: xr.Dataset, split_dims: t.List[str]) -> None:
        with FileSystems().create(self._get_output_for_dataset(dataset, split_dims)) as dest_file:
            dest_file.write(dataset.to_netcdf())

    def _get_output_for_dataset(self, dataset: xr.Dataset, split_dims: t.List[str]) -> str:
        splits = {'variable': list(dataset.data_vars.keys())[0]}
        for dim in split_dims:
            value = dataset[dim].values
            if dim == 'time':
                value = np.datetime_as_string(value, unit='m')
            splits[dim] = value
        return self.output_info.formatted_output_path(splits)


class DrySplitter(FileSplitter):

    def __init__(self, input_path: str, output_info: OutFileInfo,
                 force_split: bool = False, logging_level: int = logging.INFO):
        super().__init__(input_path, output_info,
                         force_split, logging_level)

    def split_data(self) -> None:
        if not self.output_info.split_dims():
            raise ValueError('No splitting specified in template.')
        self.logger.info('input file: %s - output scheme: %s',
                         self.input_path, self.output_info.formatted_output_path(self._get_keys()))

    def _get_keys(self) -> t.Dict[str, str]:
        return {name: name for name in self.output_info.split_dims()}


def get_splitter(file_path: str, output_info: OutFileInfo, dry_run: bool, force_split: bool = False) -> FileSplitter:
    if dry_run:
        return DrySplitter(file_path, output_info)

    with FileSystems.open(file_path) as f:
        header = f.read(4)

    if b'GRIB' in header:
        metrics.Metrics.counter('get_splitter', 'grib').inc()
        return GribSplitter(file_path, output_info, force_split)

    # See the NetCDF Spec docs:
    # https://docs.unidata.ucar.edu/netcdf-c/current/faq.html#How-can-I-tell-which-format-a-netCDF-file-uses
    if b'CDF' in header or b'HDF' in header:
        metrics.Metrics.counter('get_splitter', 'netcdf').inc()
        return NetCdfSplitter(file_path, output_info, force_split)

    raise ValueError(
        f'cannot determine if file {file_path!r} is Grib or NetCDF.')
