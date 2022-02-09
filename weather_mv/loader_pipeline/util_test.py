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
import unittest
from collections import Counter

import xarray as xr

from .sinks_test import TestDataBase
from .util import get_coordinates


class GetCoordinatesTest(TestDataBase):
    def setUp(self) -> None:
        super().setUp()
        self.test_data_path = f'{self.test_data_folder}/test_data_20180101.nc'

    def test_gets_indexed_coordinates(self):
        ds = xr.open_dataset(self.test_data_path)
        self.assertEqual(
            next(get_coordinates(ds)),
            {'latitude': 49.0, 'longitude': -108.0, 'time': '2018-01-02T06:00:00+00:00'}
        )

    def test_no_duplicate_coordinates(self):
        ds = xr.open_dataset(self.test_data_path)

        # Assert that all the coordinates are unique.
        counts = Counter([tuple(c.values()) for c in get_coordinates(ds)])
        self.assertTrue(all((c == 1 for c in counts.values())))


class ToJsonSerializableTypeTests(unittest.TestCase):
    # TODO(#106): Write tests...
    pass
