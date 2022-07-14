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
import logging
import os
import tempfile
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
        expected = 'grib_multiple_edition_single_timestep'

        actual = _get_tiff_name(uri)

        self.assertEqual(actual, expected)

    def test_tiff_name_creation__with_special_chars(self):
        uri = 'weather_mv/test_data/grib@2nd-edition&timestep#1.bz2'
        expected = 'grib_2nd-edition_timestep_1'

        actual = _get_tiff_name(uri)

        self.assertEqual(actual, expected)

    def test_tiff_name_creation__with_missing_filename(self):
        uri = 'weather_mv/test_data/'
        expected = ''

        actual = _get_tiff_name(uri)

        self.assertEqual(actual, expected)

    def test_tiff_name_creation__with_only_filename(self):
        uri = 'grib@2nd-edition&timestep#1.bz2'
        expected = 'grib_2nd-edition_timestep_1'

        actual = _get_tiff_name(uri)

        self.assertEqual(actual, expected)


class ConvertToCogTests(TestDataBase):

    def setUp(self) -> None:
        super().setUp()
        self.tmpdir = tempfile.TemporaryDirectory()
        self.convert_to_cog = ConvertToCog(tiff_location=self.tmpdir.name)

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_convert_to_cog(self):
        data_path = f'{self.test_data_folder}/test_data_grib_single_timestep'
        tiff_path = os.path.join(self.tmpdir.name, 'test_data_grib_single_timestep.tiff')

        next(self.convert_to_cog.process(data_path))

        # The size of tiff is expected to be more than grib.
        self.assertTrue(os.path.getsize(tiff_path) > os.path.getsize(data_path))

    def test_convert_to_cog__with_multiple_grib_edition(self):
        data_path = f'{self.test_data_folder}/test_data_grib_multiple_edition_single_timestep.bz2'
        tiff_path = os.path.join(self.tmpdir.name, 'test_data_grib_multiple_edition_single_timestep.tiff')

        next(self.convert_to_cog.process(data_path))

        # The size of tiff is expected to be more than grib.
        self.assertTrue(os.path.getsize(tiff_path) > os.path.getsize(data_path))


if __name__ == '__main__':
    unittest.main()
