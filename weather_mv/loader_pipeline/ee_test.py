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
import os
import logging
import unittest

from .ee import (
    _get_tiff_name,
    ConvertToCog
)
from .sinks_test import TestDataBase

logger = logging.getLogger(__name__)


class TiffNameCreationTests(unittest.TestCase):

    def test_tiff_name_creation(self):
        uri = 'weather_mv/test_data/grib_multiple_edition_single_timestep.bz2'
        actual = _get_tiff_name(uri)
        expected = 'grib_multiple_edition_single_timestep'
        self.assertEqual(actual, expected)

    def test_tiff_name_creation__with_special_chars(self):
        uri = 'weather_mv/test_data/grib@2nd-edition&timestep#1.bz2'
        actual = _get_tiff_name(uri)
        expected = 'grib_2nd-edition_timestep_1'
        self.assertEqual(actual, expected)

    def test_tiff_name_creation__with_missing_filename(self):
        uri = 'weather_mv/test_data/'
        actual = _get_tiff_name(uri)
        expected = ''
        self.assertEqual(actual, expected)

    def test_tiff_name_creation__with_only_filename(self):
        uri = 'grib@2nd-edition&timestep#1.bz2'
        actual = _get_tiff_name(uri)
        expected = 'grib_2nd-edition_timestep_1'
        self.assertEqual(actual, expected)


class ConvertToCogTests(TestDataBase):

    def setUp(self) -> None:
        super().setUp()
        self.test_tiff_dir = f'{self.test_data_folder}'  # For resultant tiff.
        self.convert_to_cog = ConvertToCog(
            tiff_location=self.test_tiff_dir
        )
        self.test_tiff_paths = []

    def tearDown(self):
        # Cleans up generated tiff files.
        for test_tiff_path in self.test_tiff_paths:
            os.remove(test_tiff_path)

    def test_convert_to_cog(self):
        self.test_data_path = f'{self.test_data_folder}/test_data_grib_single_timestep'
        test_tiff_data = self.convert_to_cog.process(self.test_data_path)
        expected_tiff_path = os.path.join(self.test_tiff_dir, f'{next(test_tiff_data).name}.tiff')
        self.assertTrue(os.path.exists(expected_tiff_path))
        self.test_tiff_paths.append(expected_tiff_path)

    def test_convert_to_cog__with_multiple_grib_edition(self):
        self.test_data_path = f'{self.test_data_folder}/test_data_grib_multiple_edition_single_timestep.bz2'
        test_tiff_data = self.convert_to_cog.process(self.test_data_path)
        expected_tiff_path = os.path.join(self.test_tiff_dir, f'{next(test_tiff_data).name}.tiff')
        self.assertTrue(os.path.exists(expected_tiff_path))
        self.test_tiff_paths.append(expected_tiff_path)


if __name__ == '__main__':
    unittest.main()
