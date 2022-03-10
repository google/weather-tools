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
import json
import xarray as xr
# from .sinks_test import TestDataBase
# from .util import get_coordinates
from util import to_json_serializable_type
import numpy as np


# class GetCoordinatesTest(TestDataBase):
#     def setUp(self) -> None:
#         super().setUp()
#         self.test_data_path = f'{self.test_data_folder}/test_data_20180101.nc'

#     def test_gets_indexed_coordinates(self):
#         ds = xr.open_dataset(self.test_data_path)
#         self.assertEqual(
#             next(get_coordinates(ds)),
#             {'latitude': 49.0, 'longitude': -108.0, 'time': '2018-01-02T06:00:00+00:00'}
#         )

#     def test_no_duplicate_coordinates(self):
#         ds = xr.open_dataset(self.test_data_path)

#         # Assert that all the coordinates are unique.
#         counts = Counter([tuple(c.values()) for c in get_coordinates(ds)])
#         self.assertTrue(all((c == 1 for c in counts.values())))


class ToJsonSerializableTypeTests(unittest.TestCase):

    def assert_value_and_type(self,input,output):
        # check if it retains the original value
        self.assertEqual(input,output)

        # check if it retains the original type
        self.assertEqual(type(input), type(output))

    def assert_serializes_json(self, input):
        try:
            json.loads(json.dumps(input))
            print("Successfully passed!")
        except json.JSONDecodeError:
            self.fail()

    def test_numpy_float_becomes_float(self):
        input = np.float(42)
        
        # genrate output from the function being tested
        output = to_json_serializable_type(input)

        # assert the output with set of given conditions

        # check if it is serialized in json 
        self.assert_serializes_json(output)

        # check if it retains the original value and type
        self.assert_value_and_type(input,output)
       

    def test_numpy_array(self):
        input = np.array([1, 2, 3, 4, 5777, 7, 4252, 5426])
        output = to_json_serializable_type(input)
        self.assert_serializes_json(output)
        self.assert_value_and_type(input,output)

    def test_strings(self):
        input = "hello there"
        output = to_json_serializable_type(input)
        self.assert_serializes_json(output)
        self.assert_value_and_type(input,output)

    def test_date_and_time(self):
        input = datetime.datetime.now().isoformat()
        output = to_json_serializable_type(input)
        self.assert_serializes_json(output)
        self.assert_value_and_type(input,output)

    def test_dictionary(self):
        input = {"a": 1, "b": 2, "c": 3, "d": 4}
        output = to_json_serializable_type(input)
        self.assert_serializes_json(output)
        self.assert_value_and_type(input,output)

    def test_dictionary(self):
        input = {"a": 1, "b": 2, "c": 3, "d": 4}
        output = to_json_serializable_type(input)
        self.assert_serializes_json(output)
        self.assert_value_and_type(input,output)

    def test_None(self):
        input = None
        output = to_json_serializable_type(input)
        self.assert_serializes_json(output)
        self.assert_value_and_type(input,output)
