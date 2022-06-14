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
import unittest
from functools import wraps

import weather_mv
from .sinks import open_dataset


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


class OpenDatasetTest(TestDataBase):

    def setUp(self) -> None:
        super().setUp()
        self.test_data_path = f'{self.test_data_folder}/test_data_20180101.nc'
        self.test_grib_path = f'{self.test_data_folder}/test_data_grib_single_timestep'
        self.test_tif_path = f'{self.test_data_folder}/test_data_tif_start_time.tif'

    def test_opens_grib_files(self):
        with open_dataset(self.test_grib_path) as ds:
            self.assertIsNotNone(ds)

    def test_accepts_xarray_kwargs(self):
        with open_dataset(self.test_data_path) as ds1:
            self.assertIn('d2m', ds1)
        with open_dataset(self.test_data_path, {'drop_variables': 'd2m'}) as ds2:
            self.assertNotIn('d2m', ds2)

    def test_opens_tif_files(self):
        with open_dataset(self.test_tif_path, tif_metadata_for_datetime='start_time') as ds:
            self.assertIsNotNone(ds)


if __name__ == '__main__':
    unittest.main()
