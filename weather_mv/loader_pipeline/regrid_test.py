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
import glob
import os.path
import tempfile
import unittest

import numpy as np
import xarray as xr
from apache_beam.testing.test_pipeline import TestPipeline
from cfgrib.xarray_to_grib import to_grib

from .regrid import Regrid
from .sinks_test import TestDataBase

try:
    import metview  # noqa
except (ModuleNotFoundError, ImportError, FileNotFoundError):
    raise unittest.SkipTest('MetView dependency is not installed. Skipping tests...')


def make_skin_temperature_dataset() -> xr.Dataset:
    ds = xr.DataArray(
        np.full((4, 5, 6), 300.),
        coords=[
            np.arange(0, 4),
            np.linspace(90., -90., 5),
            np.linspace(0., 360., 6, endpoint=False),
        ],
        dims=['time', 'latitude', 'longitude'],
    ).to_dataset(name='skin_temperature')
    ds.skin_temperature.attrs['GRIB_shortName'] = 'skt'
    ds.skin_temperature.attrs['GRIB_gridType'] = 'regular_ll'
    return ds


def metview_cache_exists() -> bool:
    caches = glob.glob(f'{tempfile.gettempdir()}/mv.*/')
    return any(os.path.isfile(p) for p in caches)


class RegridTest(TestDataBase):

    # TODO(alxr): Test the quality of the regridding...

    def setUp(self) -> None:
        super().setUp()
        self.tmpdir = tempfile.TemporaryDirectory()
        self.input_dir = os.path.join(self.tmpdir.name, 'input')
        self.input_grib = os.path.join(self.input_dir, 'test.gb')
        os.mkdir(self.input_dir)
        self.Op = Regrid(
            output_path=self.tmpdir.name,
            first_uri=self.input_grib,
            regrid_kwargs={'grid': [0.25, 0.25]},
            dry_run=False,
            zarr=False,
            zarr_kwargs={},
        )

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
        to_grib(make_skin_temperature_dataset(), self.input_grib)

        self.assertTrue(os.path.exists(self.input_grib))

        self.Op.apply(self.input_grib)

        self.assertTrue(os.path.exists(f'{self.tmpdir.name}/test.gb'))
        self.assertFalse(metview_cache_exists())

    def test_apply__works_when_called_twice(self):
        for _ in range(2):
            self.test_apply__creates_a_file()

    def test_apply__to_netCDF__creates_a_netCDF_file(self):
        to_grib(make_skin_temperature_dataset(), self.input_grib)
        self.assertTrue(os.path.exists(self.input_grib))

        Op = dataclasses.replace(self.Op, to_netcdf=True)
        Op.apply(self.input_grib)

        expected = f'{self.tmpdir.name}/test.nc'
        self.assertTrue(os.path.exists(expected))

        try:
            xr.open_dataset(expected)
        except:  # noqa
            self.fail('Cannot open netCDF with Xarray.')

    def test_zarr__coarsen(self):
        input_zarr = os.path.join(self.input_dir, 'input.zarr')
        output_zarr = os.path.join(self.input_dir, 'output.zarr')

        xr.open_dataset(os.path.join(self.test_data_folder, 'test_data_20180101.nc')).to_zarr(input_zarr)
        self.assertTrue(os.path.exists(input_zarr))

        Op = dataclasses.replace(
            self.Op,
            first_uri=input_zarr,
            output_path=output_zarr,
            zarr_input_chunks={"time": 25},
            zarr=True
        )

        with TestPipeline() as p:
            p | Op

        self.assertTrue(os.path.exists(output_zarr))
        try:
            xr.open_zarr(output_zarr)
        except:  # noqa
            self.fail('Cannot open Zarr with Xarray.')

    def test_corrupt_grib_file(self):
        correct_file_path = os.path.join(self.test_data_folder, 'test_data_grib_single_timestep')
        corrupt_file_path = os.path.join(self.test_data_folder, 'test_data_corrupt_grib')

        self.assertFalse(self.Op.is_grib_file_corrupt(correct_file_path))
        self.assertTrue(self.Op.is_grib_file_corrupt(corrupt_file_path))


if __name__ == '__main__':
    unittest.main()
