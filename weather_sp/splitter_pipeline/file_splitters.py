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
import apache_beam.metrics as metrics
import logging
import netCDF4 as nc
import pygrib
import shutil
import tempfile
import typing as t
from apache_beam.io.filesystems import FileSystems
from contextlib import contextmanager

logger = logging.getLogger(__name__)


class SplitKey(t.NamedTuple):
    level: str
    short_name: str

    def __str__(self):
        if not self.level:
            return f'field {self.short_name}'
        return f'{self.level} - field {self.short_name}'


class FileSplitter(abc.ABC):
    """Base class for weather file splitters."""

    def __init__(self, input_path: str, output_path: str, file_suffix: str = "",
                 level: int = logging.INFO):
        self.input_path = input_path
        self.output_path = output_path
        self.file_suffix = file_suffix
        self.logger = logging.getLogger(f'{__name__}.{type(self).__name__}')
        self.logger.setLevel(level)
        self.logger.debug('Splitter for path=%s, output base=%s',
                          self.input_path, self.output_path)

    @abc.abstractmethod
    def split_data(self) -> None:
        pass

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

    def _get_output_file_path(self, key: SplitKey) -> str:
        level = '_{level}'.format(level=key.level) if key.level else ''
        return '{base}{level}_{sn}.{ending}'.format(
            base=self.output_path, level=level, sn=key.short_name,
            ending=self.file_suffix)


class GribSplitter(FileSplitter):

    def __init__(self, input_path: str, output_folder: str):
        super().__init__(input_path, output_folder, file_suffix='grib')

    def split_data(self) -> None:
        outputs = dict()

        with self._open_grib_locally() as grbs:
            for grb in grbs:
                key = SplitKey(grb.typeOfLevel, grb.shortName)
                if key not in outputs:
                    metrics.Metrics.counter('file_splitters',
                                            f'grib: {key}').inc()
                    outputs[key] = self._open_outfile(key)
                outputs[key].write(grb.tostring())
                outputs[key].flush()

            for out in outputs.values():
                out.close()
            self.logger.info('split %s into %d files', self.input_path, len(outputs))

    @contextmanager
    def _open_grib_locally(self) -> t.Iterator[t.Iterator[pygrib.gribmessage]]:
        with self._copy_to_local_file() as local_file:
            yield pygrib.open(local_file.name)

    def _open_outfile(self, key: SplitKey):
        return FileSystems.create(self._get_output_file_path(key))


class NetCdfSplitter(FileSplitter):

    def __init__(self, input_path: str, output_folder: str):
        super().__init__(input_path, output_folder, file_suffix='nc')

    def split_data(self) -> None:
        with self._open_dataset_locally() as nc_data:
            fields = [var for var in nc_data.variables.keys() if
                      var not in nc_data.dimensions.keys()]
            for field in fields:
                self._create_netcdf_dataset_for_variable(nc_data, field)
            self.logger.info('split %s into %d files', self.input_path, len(fields))

    @contextmanager
    def _open_dataset_locally(self) -> t.Iterator[nc.Dataset]:
        with self._copy_to_local_file() as local_file:
            yield nc.Dataset(local_file.name, 'r')

    def _create_netcdf_dataset_for_variable(self, dataset: nc.Dataset,
                                            variable: str) -> None:
        metrics.Metrics.counter('file_splitters',
                                f'netcdf output for {variable}').inc()
        with tempfile.NamedTemporaryFile() as temp_file:
            with nc.Dataset(temp_file.name, 'w',
                            format=dataset.file_format) as dest:
                dest.setncatts(dataset.__dict__)
                for name, dim in dataset.dimensions.items():
                    dest.createDimension(
                        name,
                        (len(dim) if not dim.isunlimited() else None))
                include = [var for var in dataset.dimensions.keys()]
                include.append(variable)
                for name, var in dataset.variables.items():
                    if name in include:
                        var = dataset.variables[name]
                        dest.createVariable(name, var.datatype, var.dimensions)
                        # copy variable attributes all at once via dictionary
                        dest[name].setncatts(dataset[name].__dict__)
                        dest[name][:] = dataset[name][:]
            temp_file.flush()
            self._copy_dataset_to_storage(temp_file,
                                          self._get_output_file_path(
                                              SplitKey('', variable)))


class DrySplitter(FileSplitter):
    def __init__(self, file_path: str, output_path: str, file_ending: str):
        super().__init__(file_path, output_path, file_ending)

    def split_data(self) -> None:
        self.logger.info('input file: %s - output scheme: %s_level_shortname.%s',
                         self.input_path, self.output_path, self.file_suffix)


def get_splitter(file_path: str, output_path: str,
                 dry_run: bool) -> FileSplitter:
    if file_path.endswith('.nc') or file_path.endswith('.cd'):
        metrics.Metrics.counter('get_splitter', 'netcdf').inc()
        if dry_run:
            return DrySplitter(file_path, output_path, "nc")
        return NetCdfSplitter(file_path, output_path)
    if file_path.endswith('grb') or file_path.endswith(
            'grib') or file_path.endswith('grib2'):
        metrics.Metrics.counter('get_splitter', 'grib').inc()
    else:
        logger.info('unspecified file type, assuming grib for %s', file_path)
        metrics.Metrics.counter('get_splitter',
                                'unidentified grib').inc()
    if dry_run:
        return DrySplitter(file_path, output_path, "grib")
    return GribSplitter(file_path, output_path)
