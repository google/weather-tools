import abc
import apache_beam.metrics as metrics
import logging
import netCDF4 as nc
import os
import pygrib
import shutil
import tempfile
import typing as t
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp import gcsio
from contextlib import contextmanager

SPLIT_DIRECTORY = '/split_files/'


class SplitKey(t.NamedTuple):
    level: str
    short_name: str

    def __str__(self):
        if not self.level:
            return f'field {self.short_name}'
        return f'{self.level} - field {self.short_name}'


class FileSplitter(abc.ABC):
    """Base class for weather file splitters."""

    def __init__(self, input_path: str, file_suffix: str = "",
                 level: int = logging.INFO):
        self.input_path = input_path
        self.file_suffix = file_suffix
        self.logger = logging.getLogger(f'{__name__}.{type(self).__name__}')
        self.logger.setLevel(level)
        self.logger.info('Splitter for path=%s', self.input_path)

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

    def _get_ouput_basename(self) -> str:
        directory, file_name = os.path.split(self.input_path)
        return '{dir}{split_dir}{file}'.format(dir=directory,
                                               split_dir=SPLIT_DIRECTORY,
                                               file=file_name)

    def _copy_dataset_to_storage(self, src_file: t.IO, target: str):
        with gcsio.GcsIO().open(target, 'wb') as dest_file:
            shutil.copyfileobj(src_file, dest_file)

    def _get_output_file_path(self, key: SplitKey) -> str:
        level = '_{level}'.format(level=key.level) if key.level else ''
        return '{base}{level}_{sn}.{ending}'.format(
                base=self._get_ouput_basename(), level=level, sn=key.short_name,
                ending=self.file_suffix)


class GribSplitter(FileSplitter):

    def __init__(self, input_path: str):
        super().__init__(input_path, file_suffix='grib')

    def split_data(self) -> None:
        outputs = dict()

        grbs = self._open_grib_locally()
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
        logging.info('split %s into %d files', self.input_path, len(outputs))

    def _open_grib_locally(self) -> t.Iterator[pygrib.gribmessage]:
        with self._copy_to_local_file() as local_file:
            return pygrib.open(local_file.name)

    def _open_outfile(self, key: SplitKey):
        return gcsio.GcsIO().open(self._get_output_file_path(key), 'wb')


class NetCdfSplitter(FileSplitter):

    def __init__(self, input_path: str):
        super().__init__(input_path, file_suffix='nc')

    def split_data(self) -> None:
        nc_data = self._open_dataset_locally()
        fields = [var for var in nc_data.variables.keys() if
                  var not in nc_data.dimensions.keys()]
        for field in fields:
            self._create_netcdf_dataset_for_variable(nc_data, field)
        logging.info('split %s into %d files', self.input_path, len(fields))

    def _open_dataset_locally(self) -> nc.Dataset:
        with self._copy_to_local_file() as local_file:
            return nc.Dataset(local_file.name, 'r')

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


def get_splitter(file_path: str) -> FileSplitter:
    if file_path.endswith('.nc') or file_path.endswith('.cd'):
        metrics.Metrics.counter('get_splitter', 'netcdf').inc()
        return NetCdfSplitter(file_path)
    if file_path.endswith('grb') or file_path.endswith(
            'grib') or file_path.endswith('grib2'):
        metrics.Metrics.counter('get_splitter', 'grib').inc()
        return GribSplitter(file_path)
    logging.info('unspecified file type, assuming grib for %s', file_path)
    metrics.Metrics.counter('get_splitter',
                            'unidentified grib').inc()
    return GribSplitter(file_path)
