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
            tiff_config={"dims": []},
        )
        self.convert_to_table_asset = ConvertToAsset(
            asset_location=self.tmpdir.name,
            ee_asset_type='TABLE'
        )

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_convert_to_image_asset(self):
        data_path = f'{self.test_data_folder}/test_data_grib_single_timestep'
        asset_path = os.path.join(self.tmpdir.name, 'test_data_grib_single_timestep_20211018060000.tiff')

        next(self.convert_to_image_asset.process(data_path))

        # The size of tiff is expected to be more than grib.
        self.assertTrue(os.path.getsize(asset_path) > os.path.getsize(data_path))

    def test_convert_to_multiple_image_assets(self):
        data_path = f'{self.test_data_folder}/test_data_20180101.nc'
        # file with multiple time values will generate separate tiff files
        time_arr = ["20180102060000","20180102070000","20180102080000","20180102090000",
                    "20180102100000","20180102110000","20180102120000","20180102130000",
                    "20180102140000","20180102150000","20180102160000","20180102170000",
                    "20180102180000","20180102190000","20180102200000","20180102210000",
                    "20180102220000","20180102230000","20180103000000","20180103010000",
                    "20180103020000","20180103030000","20180103040000","20180103050000",
                    "20180103060000"]
        it = self.convert_to_image_asset.process(data_path)
        for time in time_arr:
            next(it)
            asset_path = os.path.join(self.tmpdir.name, f'test_data_20180101_{time}.tiff')
            self.assertTrue(os.path.lexists(asset_path))

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
