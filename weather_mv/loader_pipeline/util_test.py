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
import itertools
import unittest
from collections import Counter
from datetime import datetime, timezone, timedelta

import xarray
import xarray as xr
import numpy as np

from .sinks_test import TestDataBase
from .util import (
    get_coordinates,
    ichunked,
    make_attrs_ee_compatible,
    to_json_serializable_type,
)


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


class IChunksTests(TestDataBase):
    def setUp(self) -> None:
        super().setUp()
        test_data_path = f'{self.test_data_folder}/test_data_20180101.nc'
        self.items = range(20)
        self.coords = get_coordinates(xarray.open_dataset(test_data_path), test_data_path)

    def test_even_chunks(self):
        actual = []
        for chunk in ichunked(self.items, 4):
            actual.append(list(chunk))

        self.assertEqual(actual, [
            [0, 1, 2, 3],
            [4, 5, 6, 7],
            [8, 9, 10, 11],
            [12, 13, 14, 15],
            [16, 17, 18, 19],
        ])

    def test_odd_chunks(self):
        actual = []
        for chunk in ichunked(self.items, 7):
            actual.append(list(chunk))

        self.assertEqual(actual, [
            [0, 1, 2, 3, 4, 5, 6],
            [7, 8, 9, 10, 11, 12, 13],
            [14, 15, 16, 17, 18, 19]
        ])

    def test_get_coordinates(self):
        actual = []
        for chunk in ichunked(itertools.islice(self.coords, 4), 3):
            actual.append(list(chunk))

        self.assertEqual(
            actual,
            [
                [
                    {'longitude': -108.0, 'latitude': 49.0, 'time': '2018-01-02T06:00:00+00:00'},
                    {'longitude': -108.0, 'latitude': 49.0, 'time': '2018-01-02T07:00:00+00:00'},
                    {'longitude': -108.0, 'latitude': 49.0, 'time': '2018-01-02T08:00:00+00:00'},
                ],
                [
                    {'longitude': -108.0, 'latitude': 49.0, 'time': '2018-01-02T09:00:00+00:00'}
                ]
            ]
        )


class MakeAttrsEeCompatibleTests(TestDataBase):
    def test_make_attrs_ee_compatible_a1(self):
        attrs = {
            'int_attr': 48,
            'float_attr': 48.48,
            'str_attr': '48.48',
            'str_long_attr': 'Lorem ipsum dolor sit amet, consectetur '
            'adipiscing elit. Fusce bibendum odio ac lorem tristique, sed '
            'tincidunt orci ultricies. Vivamus eu rhoncus metus. Praesent '
            'vitae imperdiet sapien. Donec vel ipsum sapien. Aliquam '
            'suscipit suscipit turpis, a vehicula neque. Maecenas '
            'hendrerit, mauris eu consequat aliquam, nunc elit lacinia '
            'elit, vel accumsan ipsum ex a tellus. Pellentesque habitant '
            'morbi tristique senectus et netus et malesuada fames ac '
            'turpis egestas. Fusce a felis vel dolor lobortis vestibulum '
            'ac ac velit. Etiam vitae nibh sed justo hendrerit feugiat. '
            'Sed vulputate, turpis eget fringilla euismod, urna magna '
            'consequat turpis, at aliquam metus dolor vel tortor. Sed sit '
            'amet dolor quis libero venenatis porttitor a non odio. Morbi '
            'interdum tellus non neque placerat, vel fermentum turpis '
            'bibendum. In efficitur nunc ac leo eleifend commodo. Maecenas '
            'in tincidunt diam. In consectetur eget sapien a suscipit. '
            'Nulla porttitor ullamcorper tellus sit amet ornare. Aliquam '
            'in nibh at mauris tincidunt bibendum a a elit.',
            'bool_attr': True,
            'none_attr': None,
            'key_long_raesent_id_tincidunt_velit_Integer_eget_sapien_tincidunt_'
            'iaculis_nulla_vitae_consectetur_metus_Vestibul': 'long_string'
        }
        expected = {
            'int_attr': 48,
            'float_attr': 48.48,
            'str_attr': '48.48',
            'str_long_attr': 'Lorem ipsum dolor sit amet, consectetur '
            'adipiscing elit. Fusce bibendum odio ac lorem tristique, sed '
            'tincidunt orci ultricies. Vivamus eu rhoncus metus. Praesent '
            'vitae imperdiet sapien. Donec vel ipsum sapien. Aliquam '
            'suscipit suscipit turpis, a vehicula neque. Maecenas '
            'hendrerit, mauris eu consequat aliquam, nunc elit lacinia '
            'elit, vel accumsan ipsum ex a tellus. Pellentesque habitant '
            'morbi tristique senectus et netus et malesuada fames ac '
            'turpis egestas. Fusce a felis vel dolor lobortis vestibulum '
            'ac ac velit. Etiam vitae nibh sed justo hendrerit feugiat. '
            'Sed vulputate, turpis eget fringilla euismod, urna magna '
            'consequat turpis, at aliquam metus dolor vel tortor. Sed sit '
            'amet dolor quis libero venenatis porttitor a non odio. Morbi '
            'interdum tellus non neque placerat, vel fermentum turpis '
            'bibendum. In efficitur nunc ac leo eleifend commodo. Maecenas '
            'in tincidunt diam. In consectetur eget sapien a suscipit. '
            'Nulla porttitor ullamcorper tellus sit amet ornare. Aliquam '
            'in nibh at mauris tincidunt bibendum a a ...',
            'bool_attr': 'True',
            'none_attr': 'None',
            'key_long_raesent_id_tincidunt_velit_Integer_eget_sapien_tincidunt_'
            'iaculis_nulla_vitae_consectetur_metus_Vestib': 'long_string'
        }

        actual = make_attrs_ee_compatible(attrs)

        self.assertDictEqual(actual, expected)

    def test_make_attrs_ee_compatible_a2(self):
        attrs = {
            'list_attr': ['attr1', 'attr1'],
            'tuple_attr': ('attr1', 'attr2'),
            'dict_attr': {
                'attr1': 1,
                'attr2': 'two',
                'attr3': 3.0,
                'attr4': True
            }
        }
        expected = {
            'list_attr': "['attr1', 'attr1']",
            'tuple_attr': "('attr1', 'attr2')",
            'dict_attr': "{'attr1': 1, 'attr2': 'two', 'attr3': 3.0, "
            "'attr4': True}"
        }

        actual = make_attrs_ee_compatible(attrs)

        self.assertDictEqual(actual, expected)


class ToJsonSerializableTypeTests(unittest.TestCase):

    def _convert(self, value):
        return to_json_serializable_type(value)

    def test_to_json_serializable_type_none(self):
        self.assertIsNone(self._convert(None))
        self.assertIsNone(self._convert(float('NaN')))
        self.assertIsNone(self._convert(np.NaN))
        self.assertIsNone(self._convert(np.datetime64('NaT')))
        self.assertIsNotNone(self._convert(np.array([])))

    def test_to_json_serializable_type_float(self):
        self.assertIsInstance(self._convert(np.float32('0.1')), float)
        self.assertIsInstance(self._convert(np.float32('1')), float)
        self.assertIsInstance(self._convert(np.float16('0.1')), float)
        self.assertIsInstance(self._convert(np.single('0.1')), float)
        self.assertIsInstance(self._convert(np.double('0.1')), float)
        self.assertNotIsInstance(self._convert(1), float)
        self.assertNotIsInstance(self._convert(np.csingle('0.1')), float)
        self.assertNotIsInstance(self._convert(np.cdouble('0.1')), float)
        self.assertNotIsInstance(self._convert(np.intc('1')), float)

    def test_to_json_serializable_type_int(self):
        self.assertIsInstance(self._convert(np.int16('1')), int)
        self.assertEqual(self._convert(np.int16('1')), int(1))
        self.assertEqual(self._convert(np.int32('1')), int(1))
        self.assertEqual(self._convert(np.int64('1')), int(1))
        self.assertEqual(self._convert(np.int64(-10_000)), -10_000)
        self.assertEqual(self._convert(np.uint16(25)), 25)

    def test_to_json_serializable_type_set(self):
        self.assertEqual(self._convert(set({})), [])
        self.assertEqual(self._convert(set({1, 2, 3})), [1, 2, 3])
        self.assertEqual(self._convert(set({1})), [1])
        self.assertEqual(self._convert(set({None})), [None])
        self.assertEqual(self._convert(set({float('NaN')})), [None])

    def test_to_json_serializable_type_ndarray(self):
        self.assertIsInstance(self._convert(np.array(list(range(10)))), list)
        self.assertEqual(self._convert(np.array(list(range(10)))), list(range(10)))
        self.assertEqual(self._convert(np.array([1])), [1])
        self.assertEqual(self._convert(np.array([[1, 2, 3], [4, 5, 6]])), [[1, 2, 3], [4, 5, 6]])
        self.assertEqual(self._convert(np.array(1)), 1)

    def test_to_json_serializable_type_datetime(self):
        input_date = '2000-01-01T00:00:00+00:00'
        now = datetime.now()

        self.assertEqual(self._convert(datetime.fromisoformat(input_date)), input_date)
        self.assertEqual(self._convert(now), now.replace(tzinfo=timezone.utc).isoformat())
        self.assertEqual(self._convert(input_date), input_date)
        self.assertEqual(self._convert(np.datetime64(input_date)), input_date)
        self.assertEqual(self._convert(np.datetime64(1, 'Y')), '1971-01-01T00:00:00+00:00')
        self.assertEqual(self._convert(np.datetime64(30, 'Y')), input_date)
        self.assertEqual(self._convert(np.timedelta64(1, 'm')), float(60))
        self.assertEqual(self._convert(timedelta(seconds=1)), float(1))
        self.assertEqual(self._convert(timedelta(minutes=1)), float(60))
        self.assertEqual(self._convert(timedelta(days=1)), float(86400))
