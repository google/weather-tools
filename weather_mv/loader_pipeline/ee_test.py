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
import fnmatch
import logging
import os
import tempfile
import unittest
import xarray as xr
import numpy as np

from .ee import (
    get_ee_safe_name,
    ConvertToAsset,
    add_additional_attrs,
    partition_dataset,
    construct_asset_name
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
        self.convert_to_image_asset = ConvertToAsset(asset_location=self.tmpdir.name)
        self.convert_to_table_asset = ConvertToAsset(asset_location=self.tmpdir.name, ee_asset_type='TABLE')

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_convert_to_image_asset(self):
        data_path = f'{self.test_data_folder}/test_data_grib_single_timestep'
        asset_path = os.path.join(self.tmpdir.name, 'test_data_grib_single_timestep.tiff')

        next(self.convert_to_image_asset.process(data_path))

        # The size of tiff is expected to be more than grib.
        self.assertTrue(os.path.getsize(asset_path) > os.path.getsize(data_path))

    def test_convert_to_image_asset__with_multiple_grib_edition(self):
        data_path = f'{self.test_data_folder}/test_data_grib_multiple_edition_single_timestep.bz2'
        asset_path = os.path.join(self.tmpdir.name, 'test_data_grib_multiple_edition_single_timestep.tiff')

        next(self.convert_to_image_asset.process(data_path))

        # The size of tiff is expected to be more than grib.
        self.assertTrue(os.path.getsize(asset_path) > os.path.getsize(data_path))

    def test_convert_to_table_asset(self):
        data_path = f'{self.test_data_folder}/test_data_grib_single_timestep'
        asset_path = os.path.join(self.tmpdir.name, 'test_data_grib_single_timestep.csv')

        next(self.convert_to_table_asset.process(data_path))

        # The size of tiff is expected to be more than grib.
        self.assertTrue(os.path.getsize(asset_path) > os.path.getsize(data_path))

    def test_convert_to_table_asset__with_multiple_grib_edition(self):
        data_path = f'{self.test_data_folder}/test_data_grib_multiple_edition_single_timestep.bz2'
        asset_path = os.path.join(self.tmpdir.name, 'test_data_grib_multiple_edition_single_timestep.csv')

        next(self.convert_to_table_asset.process(data_path))

        # The size of tiff is expected to be more than grib.
        self.assertTrue(os.path.getsize(asset_path) > os.path.getsize(data_path))

    def test_convert_to_multi_image_asset(self):
        convert_to_multi_image_asset = ConvertToAsset(
            asset_location=self.tmpdir.name,
            partition_dims=['time', 'step'],
            asset_name_format='{init_time}_{valid_time}',
            dim_mapping={
                'init_time': 'time',
                'valid_time': 'step'
            }
        )
        data_path = f'{self.test_data_folder}/test_data_multi_dimension.nc'
        asset_path = os.path.join(self.tmpdir.name)
        list(convert_to_multi_image_asset.process(data_path))

        # Make sure there are total 9 tiff files generated at target location
        self.assertEqual(len(fnmatch.filter(os.listdir(asset_path), '*.tiff')), 9)


class PartitionDatasetTests(TestDataBase):

    def setUp(self):
        super().setUp()
        self.dim_mapping = {
            'init_time': 'time',
            'valid_time': 'step'
        }
        self.date_format = '%Y%m%d%H%M'
        self.ds = xr.open_dataset(f'{self.test_data_folder}/test_data_multi_dimension.nc')

    def test_add_additional_attrs(self):

        sliced_ds = self.ds.isel({'time': 0, 'step': 1})
        attrs = add_additional_attrs(sliced_ds, self.dim_mapping, self.date_format)
        attr_names = ['init_time', 'valid_time', 'start_time', 'end_time', 'forecast_seconds']

        for name in attr_names:
            self.assertTrue(name in attrs)

        self.assertEqual(attrs['init_time'], '202412010000')
        self.assertEqual(attrs['valid_time'], '202412010600')
        self.assertEqual(attrs['start_time'], '2024-12-01T00:00:00Z')
        self.assertEqual(attrs['end_time'], '2024-12-01T06:00:00Z')
        self.assertEqual(attrs['forecast_seconds'], 6 * 60 * 60)

    def test_construct_asset_name(self):

        asset_name_format = '{init_time}_{valid_time}_{level}'
        attrs = {
            'init_time': '202412010000',
            'valid_time': '202412010600',
            'level_value': np.array(2),
            'time_value': np.datetime64('2024-12-01'),
            'step_value': np.timedelta64(6, 'h')
        }

        self.assertEqual(construct_asset_name(attrs, asset_name_format), '202412010000_202412010600_2')

    def test_partition_dataset(self):

        partition_dims = ['time', 'step']
        asset_name_format = '{init_time}_{valid_time}'
        partition_datasets = partition_dataset(
            self.ds, partition_dims, self.dim_mapping, asset_name_format, self.date_format
        )

        # As the ds partitioned on time(3) and step(3), there should be total 9 datasets
        self.assertEqual(len(partition_datasets), 9)

        dates = ['202412010000', '202412020000', '202412030000']
        valid_times = [
            '202412010000', '202412010600', '202412011200',
            '202412020000', '202412020600', '202412021200',
            '202412030000', '202412030600', '202412031200',
        ]

        for i, dataset in enumerate(partition_datasets):

            # Make sure the level dimension is flattened and values added in dataArray name
            self.assertEqual(len(dataset.data_vars), 6)
            self.assertTrue('level_0_x' in dataset.data_vars)
            self.assertTrue('level_0_y' in dataset.data_vars)
            self.assertTrue('level_0_z' in dataset.data_vars)
            self.assertTrue('level_1_x' in dataset.data_vars)
            self.assertTrue('level_1_y' in dataset.data_vars)
            self.assertTrue('level_1_z' in dataset.data_vars)

            # Make sure the dataset contains only 2 dimension: latitude and longitude
            self.assertEqual(len(dataset.dims), 2)
            self.assertTrue('latitude' in dataset.dims.keys())
            self.assertTrue('longitude' in dataset.dims.keys())

            # Make sure the data arrays are of 2D
            self.assertEqual(dataset['level_0_x'].shape, (18, 36))

            # Make sure the dataset have correct name
            self.assertTrue('asset_name' in dataset.attrs)
            self.assertEqual(dataset.attrs['asset_name'], f'{dates[i // 3]}_{valid_times[i]}')


if __name__ == '__main__':
    unittest.main()
