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
import contextlib
import datetime
from functools import wraps
import numpy as np
import os
import tempfile
import tracemalloc
import unittest
import xarray as xr

import weather_mv
from .sinks import match_datetime, open_dataset


class TestDataBase(unittest.TestCase):
    def setUp(self) -> None:
        self.test_data_folder = f'{next(iter(weather_mv.__path__))}/test_data'


def _handle_missing_grib_be(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except ValueError as e:
            # Some setups may not have Cfgrib installed properly. Ignore tests for these cases.
            e_str = str(e)
            if "Consider explicitly selecting one of the installed engines" not in e_str or "cfgrib" in e_str:
                raise

    return decorated


@contextlib.contextmanager
def limit_memory(max_memory=30):
    ''''Measure memory consumption of the function.
        'memory limit' in MB
    '''
    try:
        tracemalloc.start()
        yield
    finally:
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        assert peak / 1024 ** 2 <= max_memory, f"Memory usage {peak / 1024 ** 2} exceeded {max_memory} MB limit."


@contextlib.contextmanager
def write_netcdf():
    """Generates temporary netCDF file using xarray."""
    lat_dim = 3210
    lon_dim = 3440
    lat = np.linspace(-90, 90, lat_dim)
    lon = np.linspace(-180, 180, lon_dim)
    data_arr = np.random.uniform(low=0, high=0.1, size=(5, lat_dim, lon_dim))

    ds = xr.Dataset(
        {"var_1": (('time', 'lat', 'lon'), data_arr)},
        coords={
            "lat": lat,
            "lon": lon,
        })
    with tempfile.NamedTemporaryFile() as fp:
        ds.to_netcdf(fp.name)
        yield fp.name


class OpenDatasetTest(TestDataBase):

    def setUp(self) -> None:
        super().setUp()
        self.test_data_path = os.path.join(self.test_data_folder, 'test_data_20180101.nc')
        self.test_grib_path = os.path.join(self.test_data_folder, 'test_data_grib_single_timestep')
        self.test_tif_path = os.path.join(self.test_data_folder, 'test_data_tif_time.tif')
        self.test_zarr_path = os.path.join(self.test_data_folder, 'test_data.zarr')

    def test_opens_grib_files(self):
        with open_dataset(self.test_grib_path) as ds1:
            self.assertIsNotNone(ds1)
            self.assertDictContainsSubset({'is_normalized': True}, ds1.attrs)
        with open_dataset(self.test_grib_path, disable_grib_schema_normalization=True) as ds2:
            self.assertIsNotNone(ds2)
            self.assertDictContainsSubset({'is_normalized': False}, ds2.attrs)

    def test_accepts_xarray_kwargs(self):
        with open_dataset(self.test_data_path) as ds1:
            self.assertIn('d2m', ds1)
            self.assertDictContainsSubset({'is_normalized': False}, ds1.attrs)
        with open_dataset(self.test_data_path, {'drop_variables': 'd2m'}) as ds2:
            self.assertNotIn('d2m', ds2)
            self.assertDictContainsSubset({'is_normalized': False}, ds2.attrs)

    def test_opens_tif_files(self):
        with open_dataset(self.test_tif_path, tif_metadata_for_start_time='start_time',
                            tif_metadata_for_end_time='end_time') as ds:
            self.assertIsNotNone(ds)
            self.assertDictContainsSubset({'is_normalized': False}, ds.attrs)

    def test_opens_zarr(self):
        with open_dataset(self.test_zarr_path, is_zarr=True, open_dataset_kwargs={}) as ds:
            self.assertIsNotNone(ds)
            self.assertEqual(list(ds.data_vars), ['cape', 'd2m'])

    def test_open_dataset__fits_memory_bounds(self):
        with write_netcdf() as test_netcdf_path:
            with limit_memory(max_memory=30):
                with open_dataset(test_netcdf_path) as _:
                    pass


class DatetimeTest(unittest.TestCase):

    def test_datetime_regex_string(self):
        file_name = '3B-HHR-E_MS_MRG_3IMERG_20220901-S000000-E002959_0000_V06C_30min.tiff'

        regex_str = '3B-HHR-E_MS_MRG_3IMERG_%Y%m%d-S%H%M%S-*.tiff'

        expected = datetime.datetime.strptime('2022-09-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        actual = match_datetime(file_name, regex_str)

        self.assertEqual(actual, expected)

    def test_datetime_regex_string_with_missing_parameters(self):
        file_name = '3B-HHR-E_MS_MRG_3IMERG_0901-S000000-E002959_0000_V06C_30min.tiff'

        regex_str = '3B-HHR-E_MS_MRG_3IMERG_%m%d-S%H%M%S-*.tiff'

        expected = datetime.datetime.strptime('1978-09-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        actual = match_datetime(file_name, regex_str)

        self.assertEqual(actual, expected)

    def test_datetime_regex_string_with_different_order(self):
        file_name = '3B-HHR-E_MS_MRG_3IMERG_09012022-S000000-E002959_0000_V06C_30min.tiff'

        regex_str = '3B-HHR-E_MS_MRG_3IMERG_%m%d%Y-S%H%M%S-*.tiff'

        expected = datetime.datetime.strptime('2022-09-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        actual = match_datetime(file_name, regex_str)

        self.assertEqual(actual, expected)


if __name__ == '__main__':
    unittest.main()
