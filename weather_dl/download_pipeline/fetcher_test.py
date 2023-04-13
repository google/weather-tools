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

import io
import json
import os
import tempfile
import unittest
from unittest.mock import patch

from .config import Config
from .fetcher import Fetcher
from .manifest import MockManifest, Location
from .stores import InMemoryStore


class FetchDataTest(unittest.TestCase):

    def setUp(self) -> None:
        self.dummy_manifest = MockManifest(Location('dummy-manifest'))

    @patch('weather_dl.download_pipeline.clients.CDSClientExtended.download')
    @patch('weather_dl.download_pipeline.clients.CDSClientExtended.fetch')
    def test_fetch_data(self, mock_fetch, mock_download):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config.from_dict({
                'parameters': {
                    'dataset': 'reanalysis-era5-pressure-levels',
                    'partition_keys': ['year', 'month'],
                    'target_path': os.path.join(tmpdir, 'download-{:02d}-{:02d}.nc'),
                    'api_url': 'https//api-url.com/v1/',
                    'api_key': '12345',
                },
                'selection': {
                    'features': ['pressure'],
                    'month': ['12'],
                    'year': ['01']
                }
            })

            fetcher = Fetcher('cds', self.dummy_manifest, InMemoryStore())
            fetcher.fetch_data(config)

            self.assertTrue(os.path.exists(os.path.join(tmpdir, 'download-01-12.nc')))

            mock_fetch.assert_called_with(
                config.selection,
                'reanalysis-era5-pressure-levels',
                )

    @patch('weather_dl.download_pipeline.clients.CDSClientExtended.download')
    @patch('weather_dl.download_pipeline.clients.CDSClientExtended.fetch')
    def test_fetch_data__manifest__returns_success(self, mock_fetch, mock_download):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config.from_dict({
                'parameters': {
                    'dataset': 'reanalysis-era5-pressure-levels',
                    'partition_keys': ['year', 'month'],
                    'target_path': os.path.join(tmpdir, 'download-{:02d}-{:02d}.nc'),
                    'api_url': 'https//api-url.com/v1/',
                    'api_key': '12345',
                },
                'selection': {
                    'features': ['pressure'],
                    'month': ['12'],
                    'year': ['01']
                }
            })

            fetcher = Fetcher('cds', self.dummy_manifest, InMemoryStore())
            fetcher.fetch_data(config)

            self.assertDictContainsSubset(dict(
                selection=json.dumps(config.selection),
                location=os.path.join(tmpdir, 'download-01-12.nc'),
                stage='upload',
                status='success',
                error=None,
                username='unknown',
            ), list(self.dummy_manifest.records.values())[0])

    @patch('weather_dl.download_pipeline.clients.CDSClientExtended.fetch')
    def test_fetch_data__manifest__records_retrieve_failure(self, mock_fetch):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config.from_dict({
                'parameters': {
                    'dataset': 'reanalysis-era5-pressure-levels',
                    'partition_keys': ['year', 'month'],
                    'target_path': os.path.join(tmpdir, 'download-{:02d}-{:02d}.nc'),
                    'api_url': 'https//api-url.com/v1/',
                    'api_key': '12345',
                },
                'selection': {
                    'features': ['pressure'],
                    'month': ['12'],
                    'year': ['01']
                }
            })

            error = IOError("We don't have enough permissions to download this.")
            mock_fetch.side_effect = error

            with self.assertRaises(IOError) as e:
                fetcher = Fetcher('cds', self.dummy_manifest, InMemoryStore())
                fetcher.fetch_data(config)

            actual = list(self.dummy_manifest.records.values())[0]

            self.assertDictContainsSubset(dict(
                selection=json.dumps(config.selection),
                location=os.path.join(tmpdir, 'download-01-12.nc'),
                stage='fetch',
                status='failure',
                username='unknown',
            ), actual)

            self.assertIn(error.args[0], actual['error'])
            self.assertIn(error.args[0], e.exception.args[0])

    @patch('weather_dl.download_pipeline.clients.CDSClientExtended.fetch')
    def test_fetch_data__manifest__records_gcs_failure(self, mock_fetch):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config.from_dict({
                'parameters': {
                    'dataset': 'reanalysis-era5-pressure-levels',
                    'partition_keys': ['year', 'month'],
                    'target_path': os.path.join(tmpdir, 'download-{:02d}-{:02d}.nc'),
                    'api_url': 'https//api-url.com/v1/',
                    'api_key': '12345',
                },
                'selection': {
                    'features': ['pressure'],
                    'month': ['12'],
                    'year': ['01']
                }
            })

            error = IOError("Can't open gcs file.")
            mock_fetch.side_effect = error

            with self.assertRaises(IOError) as e:
                fetcher = Fetcher('cds', self.dummy_manifest, InMemoryStore())
                fetcher.fetch_data(config)

            actual = list(self.dummy_manifest.records.values())[0]

            self.assertDictContainsSubset(dict(
                selection=json.dumps(config.selection),
                location=os.path.join(tmpdir, 'download-01-12.nc'),
                stage='fetch',
                status='failure',
                username='unknown',
            ), actual)

            self.assertIn(error.args[0], actual['error'])
            self.assertIn(error.args[0], e.exception.args[0])

    @patch('weather_dl.download_pipeline.stores.InMemoryStore.open', return_value=io.StringIO())
    @patch('weather_dl.download_pipeline.clients.CDSClientExtended.fetch')
    def test_fetch_data__skips_existing_download(self, mock_fetch, mock_gcs_file):
        with tempfile.TemporaryDirectory() as tmpdir:
            config = Config.from_dict({
                'parameters': {
                    'dataset': 'reanalysis-era5-pressure-levels',
                    'partition_keys': ['year', 'month'],
                    'target_path': os.path.join(tmpdir, 'download-{:02d}-{:02d}.nc'),
                    'api_url': 'https//api-url.com/v1/',
                    'api_key': '12345',
                },
                'selection': {
                    'features': ['pressure'],
                    'month': ['12'],
                    'year': ['01']
                }
            })

            # target file already exists in store...
            store = InMemoryStore()
            store.store[os.path.join(tmpdir, 'download-01-12.nc')] = ''

            fetcher = Fetcher('cds', self.dummy_manifest, store)
            fetcher.fetch_data(config)

            self.assertFalse(mock_gcs_file.called)
            self.assertFalse(mock_fetch.called)
