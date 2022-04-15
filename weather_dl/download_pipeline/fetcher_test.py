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
import socket
import tempfile
import typing as t
import unittest
from unittest.mock import patch, ANY

from .fetcher import Fetcher
from .manifest import MockManifest, Location
from .stores import InMemoryStore, FSStore
from .config import Config


class UploadTest(unittest.TestCase):
    def setUp(self) -> None:
        self.message = b'the quick brown fox jumped over the lazy dog'.split()
        self.store = FSStore()
        self.fetcher = Fetcher('fake', store=self.store)

    def test_upload_writes_to_store(self):
        with tempfile.NamedTemporaryFile() as src:
            src.writelines(self.message)
            src.flush()
            src.seek(0)
            with tempfile.NamedTemporaryFile('wb') as dst:
                self.fetcher.upload(src, dst.name)
                with open(dst.name, 'rb') as dst1:
                    self.assertEqual(dst1.readlines()[0], b''.join(self.message))

    def test_retries_after_socket_timeout_error(self):
        class SocketTimeoutStore(InMemoryStore):
            count = 0

            def open(self, filename: str, mode: str = 'r') -> t.IO:
                self.count += 1
                raise socket.timeout('Deliberate error.')

        socket_store = SocketTimeoutStore()

        fetcher = Fetcher('fake', store=socket_store)

        with tempfile.NamedTemporaryFile() as src:
            src.writelines(self.message)
            src.flush()
            with tempfile.NamedTemporaryFile('wb') as dst:
                with self.assertRaises(socket.timeout):
                    fetcher.upload(src, dst.name)
        self.assertEqual(socket_store.count, 8)


class FetchDataTest(unittest.TestCase):

    def setUp(self) -> None:
        self.dummy_manifest = MockManifest(Location('dummy-manifest'))

    @patch('weather_dl.download_pipeline.stores.InMemoryStore.open', return_value=io.StringIO())
    @patch('cdsapi.Client.retrieve')
    def test_fetch_data(self, mock_retrieve, mock_gcs_file):
        config = Config.from_dict({
            'parameters': {
                'dataset': 'reanalysis-era5-pressure-levels',
                'partition_keys': ['year', 'month'],
                'target_path': 'gs://weather-dl-unittest/download-{:02d}-{:02d}.nc',
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

        mock_gcs_file.assert_called_with(
            'gs://weather-dl-unittest/download-01-12.nc',
            'wb'
        )

        mock_retrieve.assert_called_with(
            'reanalysis-era5-pressure-levels',
            config.selection,
            ANY)

    @patch('weather_dl.download_pipeline.stores.InMemoryStore.open', return_value=io.StringIO())
    @patch('cdsapi.Client.retrieve')
    def test_fetch_data__manifest__returns_success(self, mock_retrieve, mock_gcs_file):
        config = Config.from_dict({
            'parameters': {
                'dataset': 'reanalysis-era5-pressure-levels',
                'partition_keys': ['year', 'month'],
                'target_path': 'gs://weather-dl-unittest/download-{:02d}-{:02d}.nc',
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
            selection=config.selection,
            location='gs://weather-dl-unittest/download-01-12.nc',
            status='success',
            error=None,
            user='unknown',
        ), list(self.dummy_manifest.records.values())[0]._asdict())

    @patch('cdsapi.Client.retrieve')
    def test_fetch_data__manifest__records_retrieve_failure(self, mock_retrieve):
        config = Config.from_dict({
            'parameters': {
                'dataset': 'reanalysis-era5-pressure-levels',
                'partition_keys': ['year', 'month'],
                'target_path': 'gs://weather-dl-unittest/download-{:02d}-{:02d}.nc',
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
        mock_retrieve.side_effect = error

        with self.assertRaises(IOError) as e:
            fetcher = Fetcher('cds', self.dummy_manifest, InMemoryStore())
            fetcher.fetch_data(config)

        actual = list(self.dummy_manifest.records.values())[0]._asdict()

        self.assertDictContainsSubset(dict(
            selection=config.selection,
            location='gs://weather-dl-unittest/download-01-12.nc',
            status='failure',
            user='unknown',
        ), actual)

        self.assertIn(error.args[0], actual['error'])
        self.assertIn(error.args[0], e.exception.args[0])

    @patch('weather_dl.download_pipeline.stores.InMemoryStore.open', return_value=io.StringIO())
    @patch('cdsapi.Client.retrieve')
    def test_fetch_data__manifest__records_gcs_failure(self, mock_retrieve, mock_gcs_file):
        config = Config.from_dict({
            'parameters': {
                'dataset': 'reanalysis-era5-pressure-levels',
                'partition_keys': ['year', 'month'],
                'target_path': 'gs://weather-dl-unittest/download-{:02d}-{:02d}.nc',
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
        mock_gcs_file.side_effect = error

        with self.assertRaises(IOError) as e:
            fetcher = Fetcher('cds', self.dummy_manifest, InMemoryStore())
            fetcher.fetch_data(config)

        actual = list(self.dummy_manifest.records.values())[0]._asdict()
        self.assertDictContainsSubset(dict(
            selection=config.selection,
            location='gs://weather-dl-unittest/download-01-12.nc',
            status='failure',
            user='unknown',
        ), actual)

        self.assertIn(error.args[0], actual['error'])
        self.assertIn(error.args[0], e.exception.args[0])

    @patch('weather_dl.download_pipeline.stores.InMemoryStore.open', return_value=io.StringIO())
    @patch('cdsapi.Client.retrieve')
    def test_fetch_data__skips_existing_download(self, mock_retrieve, mock_gcs_file):
        config = Config.from_dict({
            'parameters': {
                'dataset': 'reanalysis-era5-pressure-levels',
                'partition_keys': ['year', 'month'],
                'target_path': 'gs://weather-dl-unittest/download-{year:02d}-{month:02d}.nc',
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
        store.store['gs://weather-dl-unittest/download-01-12.nc'] = ''

        fetcher = Fetcher('cds', self.dummy_manifest, store)
        fetcher.fetch_data(config)

        self.assertFalse(mock_gcs_file.called)
        self.assertFalse(mock_retrieve.called)
