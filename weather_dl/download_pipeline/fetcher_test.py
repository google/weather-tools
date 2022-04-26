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
import apache_beam as beam
import json
import os
from apache_beam.testing.test_pipeline import TestPipeline
from unittest.mock import patch, ANY

from .fetcher import RetrieveData, FetchData, DownloadData, Upload, FetchDownloadData
from .manifest import MockManifest, Location
from .stores import InMemoryStore, FSStore
from .config import Config


class UploadTest(unittest.TestCase):
    def setUp(self) -> None:
        self.message = b'the quick brown fox jumped over the lazy dog'.split()
        self.store = FSStore()
        self.upload_obj = Upload(MockManifest(Location('dummy-manifest')), store=self.store)

    def test_upload_writes_to_store(self):
        with tempfile.NamedTemporaryFile(delete=False) as src:
            src.writelines(self.message)
            src.flush()
            src.seek(0)
            with tempfile.NamedTemporaryFile('wb') as dst:
                self.upload_obj.upload(src.name, dst.name)
                with open(dst.name, 'rb') as dst1:
                    self.assertEqual(dst1.readlines()[0], b''.join(self.message))

    def test_retries_after_socket_timeout_error(self):
        class SocketTimeoutStore(InMemoryStore):
            count = 0

            def open(self, filename: str, mode: str = 'r') -> t.IO:
                self.count += 1
                raise socket.timeout('Deliberate error.')

        socket_store = SocketTimeoutStore()

        upload_obj = Upload(MockManifest(Location('dummy-manifest')), store=socket_store)

        with tempfile.NamedTemporaryFile(delete=False) as src:
            src.writelines(self.message)
            src.flush()
            with tempfile.NamedTemporaryFile('wb') as dst:
                with self.assertRaises(socket.timeout):
                    upload_obj.upload(src.name, dst.name)
        self.assertEqual(socket_store.count, 8)


class RetrieveDataTest(unittest.TestCase):

    def setUp(self) -> None:
        self.temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.dummy_manifest = MockManifest(Location(self.temp_file.name))

    def tearDown(self) -> None:
        os.remove(self.temp_file.name)

    @patch('cdsapi.Client.retrieve')
    def test_retrieve_data(self, mock_retrieve):
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

        my_tuple = (config, 'default')
        with TestPipeline() as p:
            (
                p
                | beam.Create([iter(my_tuple)])
                | beam.ParDo(RetrieveData('cds', self.dummy_manifest, InMemoryStore()))
            )

        mock_retrieve.assert_called_with(
            'reanalysis-era5-pressure-levels',
            config.selection,
            ANY)

    @patch('cdsapi.Client.retrieve')
    def test_retrieve_data__manifest__returns_success(self, mock_retrieve):
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

        my_tuple = (config, 'default')
        with TestPipeline() as p:
            (
                p
                | beam.Create([iter(my_tuple)])
                | beam.ParDo(RetrieveData('cds', self.dummy_manifest, InMemoryStore()))
            )

        with open(self.temp_file.name, 'r') as f:
            actual = json.load(f)

        self.assertDictContainsSubset(dict(
            selection=config.selection,
            location='gs://weather-dl-unittest/download-01-12.nc',
            stage='retrieve',
            status='success',
            error=None,
            user='unknown',
        ), actual)

    @patch('cdsapi.Client.retrieve')
    def test_retrieve_data__manifest__records_retrieve_failure(self, mock_retrieve):
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
            my_tuple = (config, 'default')
            with TestPipeline() as p:
                (
                    p
                    | beam.Create([iter(my_tuple)])
                    | beam.ParDo(RetrieveData('cds', self.dummy_manifest, InMemoryStore()))
                )

        with open(self.temp_file.name, 'r') as f:
            actual = json.load(f)

        self.assertDictContainsSubset(dict(
            selection=config.selection,
            location='gs://weather-dl-unittest/download-01-12.nc',
            stage='retrieve',
            status='failure',
            user='unknown',
        ), actual)

        self.assertIn(error.args[0], actual['error'])
        self.assertIn(error.args[0], e.exception.args[0])

    @patch('weather_dl.download_pipeline.stores.InMemoryStore.open', return_value=io.StringIO())
    @patch('cdsapi.Client.retrieve')
    def test_retrieve_data__skips_existing_download(self, mock_retrieve, mock_gcs_file):
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

        my_tuple = (config, 'default')
        with TestPipeline() as p:
            (
                p
                | beam.Create([iter(my_tuple)])
                | beam.ParDo(RetrieveData('cds', self.dummy_manifest, store))
            )

        self.assertFalse(mock_gcs_file.called)
        self.assertFalse(mock_retrieve.called)


class FetchDataTest(unittest.TestCase):

    def setUp(self) -> None:
        self.temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.dummy_manifest = MockManifest(Location(self.temp_file.name))

    def tearDown(self) -> None:
        os.remove(self.temp_file.name)

    @patch('weather_dl.download_pipeline.clients.MARSECMWFServiceExtended.fetch')
    def test_fetch_data(self, mock_fetch):
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

        my_tuple = (config, 'default')
        with TestPipeline() as p:
            (
                p
                | beam.Create([iter(my_tuple)])
                | beam.ParDo(FetchData('mars', self.dummy_manifest, InMemoryStore()))
            )

        mock_fetch.assert_called_with(
            req=config.selection,
        )

    @patch('weather_dl.download_pipeline.clients.MARSECMWFServiceExtended.fetch')
    def test_fetch_data__manifest__returns_success(self, mock_fetch):
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

        my_tuple = (config, 'default')
        with TestPipeline() as p:
            (
                p
                | beam.Create([iter(my_tuple)])
                | beam.ParDo(FetchData('mars', self.dummy_manifest, InMemoryStore()))
            )

        with open(self.temp_file.name, 'r') as f:
            actual = json.load(f)

        self.assertDictContainsSubset(dict(
            selection=config.selection,
            location='gs://weather-dl-unittest/download-01-12.nc',
            stage='fetch',
            status='success',
            error=None,
            user='unknown',
        ), actual)

    @patch('weather_dl.download_pipeline.clients.MARSECMWFServiceExtended.fetch')
    def test_fetch_data__manifest__records_retrieve_failure(self, mock_fetch):
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
        mock_fetch.side_effect = error

        with self.assertRaises(IOError) as e:
            my_tuple = (config, 'default')
            with TestPipeline() as p:
                (
                    p
                    | beam.Create([iter(my_tuple)])
                    | beam.ParDo(FetchData('mars', self.dummy_manifest, InMemoryStore()))
                )

        with open(self.temp_file.name, 'r') as f:
            actual = json.load(f)

        self.assertDictContainsSubset(dict(
            selection=config.selection,
            location='gs://weather-dl-unittest/download-01-12.nc',
            stage='fetch',
            status='failure',
            user='unknown',
        ), actual)

        self.assertIn(error.args[0], actual['error'])
        self.assertIn(error.args[0], e.exception.args[0])

    @patch('weather_dl.download_pipeline.stores.InMemoryStore.open', return_value=io.StringIO())
    @patch('weather_dl.download_pipeline.clients.MARSECMWFServiceExtended.fetch')
    def test_fetch_data__skips_existing_download(self, mock_fetch, mock_gcs_file):
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

        my_tuple = (config, 'default')
        with TestPipeline() as p:
            (
                p
                | beam.Create([iter(my_tuple)])
                | beam.ParDo(FetchData('mars', self.dummy_manifest, store))
            )

        self.assertFalse(mock_gcs_file.called)
        self.assertFalse(mock_fetch.called)


class DownloadDataTest(unittest.TestCase):

    def setUp(self) -> None:
        self.temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.dummy_manifest = MockManifest(Location(self.temp_file.name))

    def tearDown(self) -> None:
        os.remove(self.temp_file.name)

    @patch('weather_dl.download_pipeline.clients.MARSECMWFServiceExtended.download')
    def test_download_data(self, mock_download):
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

        my_tuple = (config, 'default', {})
        with TestPipeline() as p:
            (
                p
                | beam.Create([iter(my_tuple)])
                | beam.ParDo(DownloadData('mars', self.dummy_manifest, InMemoryStore()))
            )

        mock_download.assert_called_with(
            res={},
            target=ANY)

    @patch('weather_dl.download_pipeline.clients.MARSECMWFServiceExtended.download')
    def test_download_data__manifest__returns_success(self, mock_download):
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

        my_tuple = (config, 'default', {})
        with TestPipeline() as p:
            (
                p
                | beam.Create([iter(my_tuple)])
                | beam.ParDo(DownloadData('mars', self.dummy_manifest, InMemoryStore()))
            )

        with open(self.temp_file.name, 'r') as f:
            actual = json.load(f)

        self.assertDictContainsSubset(dict(
            selection=config.selection,
            location='gs://weather-dl-unittest/download-01-12.nc',
            stage='download',
            status='success',
            error=None,
            user='unknown',
        ), actual)

    @patch('weather_dl.download_pipeline.clients.MARSECMWFServiceExtended.download')
    def test_download_data__manifest__records_download_failure(self, mock_download):
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
        mock_download.side_effect = error

        with self.assertRaises(IOError) as e:
            my_tuple = (config, 'default', {})
            with TestPipeline() as p:
                (
                    p
                    | beam.Create([iter(my_tuple)])
                    | beam.ParDo(DownloadData('mars', self.dummy_manifest, InMemoryStore()))
                )

        with open(self.temp_file.name, 'r') as f:
            actual = json.load(f)

        self.assertDictContainsSubset(dict(
            selection=config.selection,
            location='gs://weather-dl-unittest/download-01-12.nc',
            stage='download',
            status='failure',
            user='unknown',
        ), actual)

        self.assertIn(error.args[0], actual['error'])
        self.assertIn(error.args[0], e.exception.args[0])


class FetchDownloadDataTest(unittest.TestCase):

    def setUp(self) -> None:
        self.temp_file = tempfile.NamedTemporaryFile(delete=False)
        self.dummy_manifest = MockManifest(Location(self.temp_file.name))

    def tearDown(self) -> None:
        os.remove(self.temp_file.name)

    @patch('weather_dl.download_pipeline.clients.MARSECMWFServiceExtended.download')
    @patch('weather_dl.download_pipeline.clients.MARSECMWFServiceExtended.fetch')
    def test_fetch_download_data(self, mock_fetch, mock_download):
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

        my_tuple = (config, 'default')
        with TestPipeline() as p:
            (
                p
                | beam.Create([iter(my_tuple)])
                | beam.ParDo(FetchDownloadData('mars', self.dummy_manifest, InMemoryStore()))
            )

        mock_fetch.assert_called_with(
            req=config.selection,
        )
        mock_download.assert_called_with(
            res=ANY,
            target=ANY
        )

    @patch('weather_dl.download_pipeline.clients.MARSECMWFServiceExtended.download')
    @patch('weather_dl.download_pipeline.clients.MARSECMWFServiceExtended.fetch')
    def test_fetch_download_data__manifest__returns_success(self, mock_fetch, mock_download):
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

        my_tuple = (config, 'default')
        with TestPipeline() as p:
            (
                p
                | beam.Create([iter(my_tuple)])
                | beam.ParDo(FetchDownloadData('mars', self.dummy_manifest, InMemoryStore()))
            )

        with open(self.temp_file.name, 'r') as f:
            actual = json.load(f)

        self.assertDictContainsSubset(dict(
            selection=config.selection,
            location='gs://weather-dl-unittest/download-01-12.nc',
            stage='fetch+download',
            status='success',
            error=None,
            user='unknown',
        ), actual)

    @patch('weather_dl.download_pipeline.clients.MARSECMWFServiceExtended.download')
    @patch('weather_dl.download_pipeline.clients.MARSECMWFServiceExtended.fetch')
    def test_fetch_download_data__manifest__records_failure(self, mock_fetch, mock_download):
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
        mock_fetch.side_effect = error

        with self.assertRaises(IOError) as e:
            my_tuple = (config, 'default')
            with TestPipeline() as p:
                (
                    p
                    | beam.Create([iter(my_tuple)])
                    | beam.ParDo(FetchDownloadData('mars', self.dummy_manifest, InMemoryStore()))
                )

        with open(self.temp_file.name, 'r') as f:
            actual = json.load(f)

        self.assertDictContainsSubset(dict(
            selection=config.selection,
            location='gs://weather-dl-unittest/download-01-12.nc',
            stage='fetch+download',
            status='failure',
            user='unknown',
        ), actual)

        self.assertIn(error.args[0], actual['error'])
        self.assertIn(error.args[0], e.exception.args[0])

    @patch('weather_dl.download_pipeline.stores.InMemoryStore.open', return_value=io.StringIO())
    @patch('weather_dl.download_pipeline.clients.MARSECMWFServiceExtended.download')
    @patch('weather_dl.download_pipeline.clients.MARSECMWFServiceExtended.fetch')
    def test_fetch_download_data__skips_existing_download(self, mock_fetch, mock_download, mock_gcs_file):
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

        my_tuple = (config, 'default')
        with TestPipeline() as p:
            (
                p
                | beam.Create([iter(my_tuple)])
                | beam.ParDo(FetchDownloadData('mars', self.dummy_manifest, store))
            )

        self.assertFalse(mock_gcs_file.called)
        self.assertFalse(mock_fetch.called)
        self.assertFalse(mock_download.called)
