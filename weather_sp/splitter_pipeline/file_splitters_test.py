import numpy as np
import os
import pygrib
import shutil
import unittest
import xarray as xr
from collections import defaultdict
from unittest.mock import patch

import weather_sp
from .file_splitters import GribSplitter
from .file_splitters import NetCdfSplitter
from .file_splitters import SplitKey
from .file_splitters import get_splitter


class GetSplitterTest(unittest.TestCase):

    def test_get_splitter_grib(self):
        splitter = get_splitter('some/file/path/data.grib')
        self.assertIsInstance(splitter, GribSplitter)

    def test_get_splitter_nc(self):
        splitter = get_splitter('some/file/path/data.nc')
        self.assertIsInstance(splitter, NetCdfSplitter)

    def test_get_splitter_undetermined(self):
        splitter = get_splitter('some/file/path/data')
        self.assertIsInstance(splitter, GribSplitter)


class GribSplitterTest(unittest.TestCase):

    def setUp(self):
        self._data_dir = f'{next(iter(weather_sp.__path__))}/test_data'

    def tearDown(self):
        split_dir = f'{self._data_dir}/split_files/'
        if os.path.exists(split_dir):
            shutil.rmtree(split_dir)

    def test_get_output_file_path(self):
        splitter = GribSplitter('path/input')
        out = splitter._get_output_file_path(SplitKey('level', 'cc'))
        self.assertEqual(out, 'path/split_files/input_level_cc.grib')

    @patch('apache_beam.io.filesystems.FileSystems.create')
    def test_open_outfile(self, mock_io):
        splitter = GribSplitter('path/input')
        splitter._open_outfile(SplitKey('level', 'cc'))
        mock_io.assert_called_with('path/split_files/input_level_cc.grib')

    def test_split_data(self):
        input_path = f'{self._data_dir}/era5_sample.grib'
        splitter = GribSplitter(input_path)
        splitter.split_data()
        self.assertTrue(os.path.exists(f'{self._data_dir}/split_files/'))

        short_names = ['z', 'r', 'cc', 'd']
        input_data = defaultdict(list)
        split_data = defaultdict(list)

        input_grbs = pygrib.open(input_path)
        for grb in input_grbs:
            input_data[grb.shortName].append(grb.values)

        for sn in short_names:
            split_file = f'{self._data_dir}/split_files/era5_sample.grib_isobaricInhPa_{sn}.grib'
            split_grbs = pygrib.open(split_file)
            for grb in split_grbs:
                split_data[sn].append(grb.values)

        for sn in short_names:
            orig = np.array(input_data[sn])
            split = np.array(split_data[sn])
            self.assertEqual(orig.shape, split.shape)
            np.testing.assert_allclose(orig, split)


class NetCdfSplitterTest(unittest.TestCase):

    def setUp(self):
        self._data_dir = f'{next(iter(weather_sp.__path__))}/test_data'

    def tearDown(self):
        split_dir = f'{self._data_dir}/split_files/'
        if os.path.exists(split_dir):
            shutil.rmtree(split_dir)

    def test_get_output_file_path(self):
        splitter = NetCdfSplitter('path/input')
        out = splitter._get_output_file_path(SplitKey('', 'cc'))
        self.assertEqual(out, 'path/split_files/input_cc.nc')

    def test_split_data(self):
        input_path = f'{self._data_dir}/era5_sample.nc'
        splitter = NetCdfSplitter(input_path)
        splitter.split_data()
        self.assertTrue(os.path.exists(f'{self._data_dir}/split_files/'))
        input_data = xr.open_dataset(input_path, engine='netcdf4')
        for sn in ['d', 'cc', 'z']:
            split_file = f'{self._data_dir}/split_files/era5_sample.nc_{sn}.nc'
            split_data = xr.open_dataset(split_file, engine='netcdf4')
            xr.testing.assert_allclose(input_data[sn], split_data[sn])


if __name__ == '__main__':
    unittest.main()
