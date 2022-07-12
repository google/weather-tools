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
import dataclasses
import os.path
import tempfile
import unittest
import glob

import numpy as np
import xarray as xr
from cfgrib.xarray_to_grib import to_grib

from .regrid import Regrid
from .sinks_test import TestDataBase

try:
    import metview  # noqa
except (ModuleNotFoundError, ImportError, FileNotFoundError):
    raise unittest.SkipTest('MetView dependency is not installed. Skipping tests...')


def make_skin_temperature_dataset() -> xr.Dataset:
    ds = xr.DataArray(
        np.zeros((5, 6)) + 300.,
        coords=[
            np.linspace(90., -90., 5),
            np.linspace(0., 360., 6, endpoint=False),
        ],
        dims=['latitude', 'longitude'],
    ).to_dataset(name='skin_temperature')
    ds.skin_temperature.attrs['GRIB_shortName'] = 'skt'
    return ds


def metview_cache_exists() -> bool:
    caches = glob.glob(f'{tempfile.gettempdir()}/mv*/')
    return len(caches) > 1


class RegridTest(TestDataBase):

    # TODO(alxr): Test the quality of the regridding...

    def setUp(self) -> None:
        super().setUp()
        self.tmpdir = tempfile.TemporaryDirectory()
        self.input_dir = os.path.join(self.tmpdir.name, 'input')
        os.mkdir(self.input_dir)
        self.Op = Regrid(output_path=self.tmpdir.name, regrid_kwargs={'grid': [0.25, 0.25]}, dry_run=False)

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def test_target_name(self):
        actual = self.Op.target_from('path/to/data/called/foobar.gb')
        self.assertEqual(actual, f'{self.tmpdir.name}/foobar.gb')

    def test_target_name__to_netCDF__changes_ext(self):
        Op = dataclasses.replace(self.Op, to_netcdf=True)
        actual = Op.target_from('path/to/data/called/foobar.gb')
        self.assertEqual(actual, f'{self.tmpdir.name}/foobar.nc')

    def test_apply__creates_a_file(self):
        path = os.path.join(self.input_dir, 'test.gb')
        to_grib(make_skin_temperature_dataset(), path)

        self.assertTrue(os.path.exists(path))

        self.Op.apply(path)

        self.assertTrue(os.path.exists(f'{self.tmpdir.name}/test.gb'))
        self.assertFalse(metview_cache_exists())

    def test_apply__works_when_called_twice(self):
        for _ in range(2):
            self.test_apply__creates_a_file()

    def test_apply__to_netCDF__creates_a_netCDF_file(self):
        path = os.path.join(self.input_dir, 'test.gb')
        to_grib(make_skin_temperature_dataset(), path)
        self.assertTrue(os.path.exists(path))

        Op = dataclasses.replace(self.Op, to_netcdf=True)
        Op.apply(path)

        expected = f'{self.tmpdir.name}/test.nc'
        self.assertTrue(os.path.exists(expected))

        try:
            xr.open_dataset(expected)
        except:  # noqa
            self.fail('Cannot open netCDF with Xarray.')


if __name__ == '__main__':
    unittest.main()
