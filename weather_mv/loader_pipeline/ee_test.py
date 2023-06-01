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
    get_ee_safe_name,
    ConvertToAsset
)
from .sinks_test import TestDataBase

logger = logging.getLogger(__name__)


class AssetNameCreationTests(unittest.TestCase):

    def test_asset_name_creation(self):
        uri = 'weather_mv/test_data/grib_multiple_edition_single_timestep.bz2'
        expected = 'grib_multiple_edition_single_timestep'

        actual = get_ee_safe_name(uri)

        self.assertEqual(actual, expected)

    def test_asset_name_creation__with_special_chars(self):
        uri = 'weather_mv/test_data/grib@2nd-edition&timestep#1.bz2'
        expected = 'grib_2nd-edition_timestep_1'

        actual = get_ee_safe_name(uri)

        self.assertEqual(actual, expected)

    def test_asset_name_creation__with_missing_filename(self):
        uri = 'weather_mv/test_data/'
        expected = ''

        actual = get_ee_safe_name(uri)

        self.assertEqual(actual, expected)

    def test_asset_name_creation__with_only_filename(self):
        uri = 'grib@2nd-edition&timestep#1.bz2'
        expected = 'grib_2nd-edition_timestep_1'

        actual = get_ee_safe_name(uri)

        self.assertEqual(actual, expected)


class ConvertToAssetTests(TestDataBase):

    def setUp(self) -> None:
        super().setUp()
        self.tmpdir = tempfile.TemporaryDirectory()
        self.convert_to_image_asset = ConvertToAsset(
            asset_location=self.tmpdir.name,
            tiff_config={"dims": [], "individual_assets": False},
        )
        self.convert_to_table_asset = ConvertToAsset(
            asset_location=self.tmpdir.name,
            ee_asset_type='TABLE',
            tiff_config={"dims": [], "individual_assets": False},
        )

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_convert_to_image_asset(self):
        data_path = f'{self.test_data_folder}/test_data_grib_single_timestep'
        asset_path = os.path.join(self.tmpdir.name, 'test_data_grib_single_timestep_20211018060000.tiff')

        next(self.convert_to_image_asset.process(data_path))

        # The size of tiff is expected to be more than grib.
        self.assertTrue(os.path.getsize(asset_path) > os.path.getsize(data_path))

    def test_convert_to_image_asset__with_multiple_grib_edition(self):
        data_path = f'{self.test_data_folder}/test_data_grib_multiple_edition_single_timestep.bz2'
        asset_path = os.path.join(
            self.tmpdir.name,
            'test_data_grib_multiple_edition_single_timestep_20211210120000_FH-8.tiff',
        )

        next(self.convert_to_image_asset.process(data_path))

        # The size of tiff is expected to be more than grib.
        self.assertTrue(os.path.getsize(asset_path) > os.path.getsize(data_path))

    def test_convert_to_table_asset(self):
        data_path = f'{self.test_data_folder}/test_data_grib_single_timestep'
        asset_path = os.path.join(self.tmpdir.name, 'test_data_grib_single_timestep_20211018060000.csv')

        next(self.convert_to_table_asset.process(data_path))

        # The size of tiff is expected to be more than grib.
        self.assertTrue(os.path.getsize(asset_path) > os.path.getsize(data_path))

    def test_convert_to_table_asset__with_multiple_grib_edition(self):
        data_path = f'{self.test_data_folder}/test_data_grib_multiple_edition_single_timestep.bz2'
        asset_path = os.path.join(
            self.tmpdir.name,
            'test_data_grib_multiple_edition_single_timestep_20211210120000_FH-8.csv',
        )

        next(self.convert_to_table_asset.process(data_path))

        # The size of tiff is expected to be more than grib.
        self.assertTrue(os.path.getsize(asset_path) > os.path.getsize(data_path))


if __name__ == '__main__':
    unittest.main()
