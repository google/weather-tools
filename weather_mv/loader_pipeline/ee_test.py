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
from pathlib import PurePosixPath
import tempfile
import unittest
from unittest.mock import patch, MagicMock

import ee

from .ee import (
    get_ee_safe_name,
    ConvertToAsset,
    IngestIntoEETransform,
    AssetData
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


class IngestIntoEETransformTests(unittest.TestCase):

    @patch('weather_mv.loader_pipeline.ee.create_ee_asset_recursive')
    @patch('weather_mv.loader_pipeline.ee.create_ee_asset')
    @patch('weather_mv.loader_pipeline.ee.AuthorizedSession')
    @patch('weather_mv.loader_pipeline.ee.get_creds')
    def test_start_ingestion__create_folder_true(self, mock_get_creds, mock_session_class, mock_create_asset, mock_create_recursive):
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"name": "operation-id"}'
        mock_session.post.return_value = mock_response

        transform = IngestIntoEETransform(
            ee_asset='projects/my-project/assets/my-collection',
            ee_asset_type='IMAGE',
            ee_qps=10,
            ee_latency=0.5,
            ee_max_concurrent=10,
            private_key='',
            service_account='',
            use_personal_account=False,
            ingest_as_virtual_asset=False,
            use_metrics=False,
            create_folder_instead_of_image_collection=True
        )
        transform.check_setup = MagicMock()

        asset_data = AssetData(
            name='my_asset',
            target_path='gs://my-bucket/my_asset.tiff',
            channel_names=['b1'],
            start_time=0,
            end_time=0,
            properties={}
        )

        transform.start_ingestion(asset_data)

        mock_create_recursive.assert_called_once_with(PurePosixPath('projects/my-project/assets/my-collection'))
        mock_create_asset.assert_not_called()

    @patch('weather_mv.loader_pipeline.ee.create_ee_asset_recursive')
    @patch('weather_mv.loader_pipeline.ee.create_ee_asset')
    @patch('weather_mv.loader_pipeline.ee.AuthorizedSession')
    @patch('weather_mv.loader_pipeline.ee.get_creds')
    def test_start_ingestion__create_folder_false_with_subfolder(self, mock_get_creds, mock_session_class, mock_create_asset, mock_create_recursive):
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"name": "operation-id"}'
        mock_session.post.return_value = mock_response

        transform = IngestIntoEETransform(
            ee_asset='projects/my-project/assets/folder1/my-collection',
            ee_asset_type='IMAGE',
            ee_qps=10,
            ee_latency=0.5,
            ee_max_concurrent=10,
            private_key='',
            service_account='',
            use_personal_account=False,
            ingest_as_virtual_asset=False,
            use_metrics=False,
            create_folder_instead_of_image_collection=False
        )
        transform.check_setup = MagicMock()

        asset_data = AssetData(
            name='my_asset',
            target_path='gs://my-bucket/my_asset.tiff',
            channel_names=['b1'],
            start_time=0,
            end_time=0,
            properties={}
        )

        transform.start_ingestion(asset_data)

        mock_create_recursive.assert_called_once_with(PurePosixPath('projects/my-project/assets/folder1'))
        mock_create_asset.assert_called_once_with(PurePosixPath('projects/my-project/assets/folder1/my-collection'), ee.data.ASSET_TYPE_IMAGE_COLL)

    @patch('weather_mv.loader_pipeline.ee.create_ee_asset_recursive')
    @patch('weather_mv.loader_pipeline.ee.create_ee_asset')
    @patch('weather_mv.loader_pipeline.ee.AuthorizedSession')
    @patch('weather_mv.loader_pipeline.ee.get_creds')
    def test_start_ingestion__create_folder_false_without_subfolder(self, mock_get_creds, mock_session_class, mock_create_asset, mock_create_recursive):
        mock_session = MagicMock()
        mock_session_class.return_value = mock_session
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = '{"name": "operation-id"}'
        mock_session.post.return_value = mock_response

        transform = IngestIntoEETransform(
            ee_asset='projects/my-project/assets/my-collection',
            ee_asset_type='IMAGE',
            ee_qps=10,
            ee_latency=0.5,
            ee_max_concurrent=10,
            private_key='',
            service_account='',
            use_personal_account=False,
            ingest_as_virtual_asset=False,
            use_metrics=False,
            create_folder_instead_of_image_collection=False
        )
        transform.check_setup = MagicMock()

        asset_data = AssetData(
            name='my_asset',
            target_path='gs://my-bucket/my_asset.tiff',
            channel_names=['b1'],
            start_time=0,
            end_time=0,
            properties={}
        )

        transform.start_ingestion(asset_data)

        mock_create_recursive.assert_not_called()
        mock_create_asset.assert_called_once_with(PurePosixPath('projects/my-project/assets/my-collection'), ee.data.ASSET_TYPE_IMAGE_COLL)


if __name__ == '__main__':
    unittest.main()
