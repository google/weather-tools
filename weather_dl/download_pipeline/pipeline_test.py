# Copyright 2021 Google LLC
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
import unittest
from collections import OrderedDict
from unittest.mock import patch, ANY, MagicMock

from .stores import InMemoryStore
from .manifest import MockManifest, Location
from .pipeline import fetch_data
from .pipeline import assemble_partition_config, prepare_partitions
from .pipeline import prepare_target_name
from .pipeline import skip_partition


class PreparePartitionTest(unittest.TestCase):

    def setUp(self) -> None:
        self.dummy_manifest = MockManifest(Location('mock://dummy'))

    def test_partition_single_key(self):
        config = {
            'parameters': {
                'partition_keys': ['year'],
                'target_path': 'download-{}.nc',
            },
            'selection': {
                'features': ['pressure', 'temperature', 'wind_speed_U', 'wind_speed_V'],
                'month': [str(i) for i in range(1, 13)],
                'year': [str(i) for i in range(2015, 2021)]
            }
        }

        actual = self.create_partition_configs(config)

        self.assertListEqual([d['selection'] for d in actual], [
            {**config['selection'], **{'year': [str(i)]}}
            for i in range(2015, 2021)
        ])

    def test_partition_multi_key(self):
        config = {
            'parameters': {
                'partition_keys': ['year', 'month'],
                'target_path': 'download-{}-{}.nc',
            },
            'selection': {
                'features': ['pressure', 'temperature', 'wind_speed_U', 'wind_speed_V'],
                'month': [str(i) for i in range(1, 3)],
                'year': [str(i) for i in range(2015, 2017)]
            }
        }

        actual = self.create_partition_configs(config)

        self.assertListEqual([d['selection'] for d in actual], [
            {**config['selection'], **{'year': ['2015'], 'month': ['1']}},
            {**config['selection'], **{'year': ['2015'], 'month': ['2']}},
            {**config['selection'], **{'year': ['2016'], 'month': ['1']}},
            {**config['selection'], **{'year': ['2016'], 'month': ['2']}},
        ])

    def test_partition_multi_params_multi_key(self):
        config = {
            'parameters': OrderedDict(
                partition_keys=['year', 'month'],
                target_path='download-{}-{}.nc',
                research={
                    'api_key': 'KKKK1',
                    'api_url': 'UUUU1'
                },
                cloud={
                    'api_key': 'KKKK2',
                    'api_url': 'UUUU2'
                },
                deepmind={
                    'api_key': 'KKKK3',
                    'api_url': 'UUUU3'
                }
            ),
            'selection': {
                'features': ['pressure', 'temperature', 'wind_speed_U', 'wind_speed_V'],
                'month': [str(i) for i in range(1, 3)],
                'year': [str(i) for i in range(2015, 2017)]
            }
        }

        actual = self.create_partition_configs(config)

        self.assertListEqual(actual, [
            {'parameters': OrderedDict(config['parameters'], api_key='KKKK1',
                                       api_url='UUUU1'),
             'selection': {**config['selection'],
                           **{'year': ['2015'], 'month': ['1']}}},
            {'parameters': OrderedDict(config['parameters'], api_key='KKKK2',
                                       api_url='UUUU2'),
             'selection': {**config['selection'],
                           **{'year': ['2015'], 'month': ['2']}}},
            {'parameters': OrderedDict(config['parameters'], api_key='KKKK3',
                                       api_url='UUUU3'),
             'selection': {**config['selection'],
                           **{'year': ['2016'], 'month': ['1']}}},
            {'parameters': OrderedDict(config['parameters'], api_key='KKKK1',
                                       api_url='UUUU1'),
             'selection': {**config['selection'],
                           **{'year': ['2016'], 'month': ['2']}}},
        ])

    def test_prepare_partition_records_download_status_to_manifest(self):
        config = {
            'parameters': {
                'partition_keys': ['year'],
                'target_path': 'download-{}.nc',
            },
            'selection': {
                'features': ['pressure', 'temperature', 'wind_speed_U', 'wind_speed_V'],
                'month': [str(i) for i in range(1, 13)],
                'year': [str(i) for i in range(2015, 2021)]
            }
        }

        self.create_partition_configs(config)

        self.assertListEqual(
            [d.selection for d in self.dummy_manifest.records.values()], [
                {**config['selection'], **{'year': [str(i)]}}
                for i in range(2015, 2021)
            ])

        self.assertTrue(
            all([d.status == 'scheduled' for d in self.dummy_manifest.records.values()])
        )

    def create_partition_configs(self, config):
        partition_list = prepare_partitions(config)
        return [
            assemble_partition_config(p, config, manifest=self.dummy_manifest)
            for p in partition_list
                ]


class FetchDataTest(unittest.TestCase):

    def setUp(self) -> None:
        self.dummy_manifest = MockManifest(Location('dummy-manifest'))

    @patch('weather_dl.download_pipeline.stores.InMemoryStore.open', return_value=io.StringIO())
    @patch('cdsapi.Client.retrieve')
    def test_fetch_data(self, mock_retrieve, mock_gcs_file):
        config = {
            'parameters': {
                'dataset': 'reanalysis-era5-pressure-levels',
                'partition_keys': ['year', 'month'],
                'target_path': 'gs://weather-dl-unittest/download-{}-{}.nc',
                'api_url': 'https//api-url.com/v1/',
                'api_key': '12345',
            },
            'selection': {
                'features': ['pressure'],
                'month': ['12'],
                'year': ['01']
            }
        }

        fetch_data(config, client_name='cds', manifest=self.dummy_manifest,
                   store=InMemoryStore())

        mock_gcs_file.assert_called_with(
            'gs://weather-dl-unittest/download-01-12.nc',
            'wb'
        )

        mock_retrieve.assert_called_with(
            'reanalysis-era5-pressure-levels',
            config['selection'],
            ANY)

    @patch('weather_dl.download_pipeline.stores.InMemoryStore.open', return_value=io.StringIO())
    @patch('cdsapi.Client.retrieve')
    def test_fetch_data__manifest__returns_success(self, mock_retrieve, mock_gcs_file):
        config = {
            'parameters': {
                'dataset': 'reanalysis-era5-pressure-levels',
                'partition_keys': ['year', 'month'],
                'target_path': 'gs://weather-dl-unittest/download-{}-{}.nc',
                'api_url': 'https//api-url.com/v1/',
                'api_key': '12345',
            },
            'selection': {
                'features': ['pressure'],
                'month': ['12'],
                'year': ['01']
            }
        }

        fetch_data(config, client_name='cds', manifest=self.dummy_manifest,
                   store=InMemoryStore())

        self.assertDictContainsSubset(dict(
            selection=config['selection'],
            location='gs://weather-dl-unittest/download-01-12.nc',
            status='success',
            error=None,
            user='unknown',
        ), list(self.dummy_manifest.records.values())[0]._asdict())

    @patch('weather_dl.download_pipeline.stores.InMemoryStore.open', return_value=io.StringIO())
    @patch('cdsapi.Client.retrieve')
    def test_fetch_data__manifest__records_retrieve_failure(self, mock_retrieve,
                                                            mock_gcs_file):
        config = {
            'parameters': {
                'dataset': 'reanalysis-era5-pressure-levels',
                'partition_keys': ['year', 'month'],
                'target_path': 'gs://weather-dl-unittest/download-{}-{}.nc',
                'api_url': 'https//api-url.com/v1/',
                'api_key': '12345',
            },
            'selection': {
                'features': ['pressure'],
                'month': ['12'],
                'year': ['01']
            }
        }

        error = IOError("We don't have enough permissions to download this.")
        mock_retrieve.side_effect = error

        with self.assertRaises(IOError) as e:
            fetch_data(
                config,
                client_name='cds',
                manifest=self.dummy_manifest,
                store=InMemoryStore()
            )

            actual = list(self.dummy_manifest.records.values())[0]._asdict()

            self.assertDictContainsSubset(dict(
                selection=config['selection'],
                location='gs://weather-dl-unittest/download-01-12.nc',
                status='failure',
                user='unknown',
            ), actual)

            self.assertIn(error.args[0], actual['error'])
            self.assertIn(error.args[0], e.exception.args[0])

    @patch('weather_dl.download_pipeline.stores.InMemoryStore.open', return_value=io.StringIO())
    @patch('cdsapi.Client.retrieve')
    def test_fetch_data__manifest__records_gcs_failure(self, mock_retrieve,
                                                       mock_gcs_file):
        config = {
            'parameters': {
                'dataset': 'reanalysis-era5-pressure-levels',
                'partition_keys': ['year', 'month'],
                'target_path': 'gs://weather-dl-unittest/download-{}-{}.nc',
                'api_url': 'https//api-url.com/v1/',
                'api_key': '12345',
            },
            'selection': {
                'features': ['pressure'],
                'month': ['12'],
                'year': ['01']
            }
        }

        error = IOError("Can't open gcs file.")
        mock_gcs_file.side_effect = error

        with self.assertRaises(IOError) as e:
            fetch_data(
                config,
                client_name='cds',
                manifest=self.dummy_manifest,
                store=InMemoryStore()
            )

            actual = list(self.dummy_manifest.records.values())[0]._asdict()
            self.assertDictContainsSubset(dict(
                selection=config['selection'],
                location='gs://weather-dl-unittest/download-01-12.nc',
                status='failure',
                user='unknown',
            ), actual)

            self.assertIn(error.args[0], actual['error'])
            self.assertIn(error.args[0], e.exception.args[0])


class SkipPartitionsTest(unittest.TestCase):

    def setUp(self) -> None:
        self.mock_store = InMemoryStore()

    def test_skip_partition_missing_force_download(self):
        config = {
            'parameters': {
                'partition_keys': ['year', 'month'],
                'target_path': 'download-{}-{}.nc',
            },
            'selection': {
                'features': ['pressure', 'temperature', 'wind_speed_U', 'wind_speed_V'],
                'month': [str(i) for i in range(1, 13)],
                'year': [str(i) for i in range(2015, 2021)]
            }
        }

        actual = skip_partition(config, self.mock_store)

        self.assertEqual(actual, False)

    def test_skip_partition_force_download_true(self):
        config = {
            'parameters': {
                'partition_keys': ['year', 'month'],
                'target_path': 'download-{}-{}.nc',
                'force_download': True
            },
            'selection': {
                'features': ['pressure', 'temperature', 'wind_speed_U', 'wind_speed_V'],
                'month': [str(i) for i in range(1, 13)],
                'year': [str(i) for i in range(2015, 2021)]
            }
        }

        actual = skip_partition(config, self.mock_store)

        self.assertEqual(actual, False)

    def test_skip_partition_force_download_false(self):
        config = {
            'parameters': {
                'partition_keys': ['year', 'month'],
                'target_path': 'download-{}-{}.nc',
                'force_download': False
            },
            'selection': {
                'features': ['pressure'],
                'month': ['12'],
                'year': ['02']
            }
        }

        self.mock_store.exists = MagicMock(return_value=True)

        actual = skip_partition(config, self.mock_store)

        self.assertEqual(actual, True)


class PrepareTargetNameTest(unittest.TestCase):

    def setUp(self) -> None:
        self.dummy_manifest = MockManifest(Location('dummy-manifest'))

    def test_target_name_no_date(self):
        config = {
            'parameters': {
                'partition_keys': ['year', 'month'],
                'target_path': 'download-{}-{}.nc',
                'force_download': False
            },
            'selection': {
                'features': ['pressure'],
                'month': ['12'],
                'year': ['02']
            }
        }
        target_name = prepare_target_name(config)
        self.assertEqual(target_name, "download-02-12.nc")

    def test_target_name_date_no_target_directory(self):
        config = {
            'parameters': {
                'partition_keys': ['date'],
                'target_path': 'download-{}.nc',
                'force_download': False
            },
            'selection': {
                'features': ['pressure'],
                'date': ['2017-01-15'],
            }
        }
        target_name = prepare_target_name(config)
        self.assertEqual(target_name, "download-2017-01-15.nc")

    def test_target_name_target_directory_no_date(self):
        config = {
            'parameters': {
                'target_path': 'somewhere/',
                'partition_keys': ['year', 'month'],
                'target_filename': 'download/{}/{}.nc',
                'force_download': False
            },
            'selection': {
                'features': ['pressure'],
                'month': ['12'],
                'year': ['02']
            }
        }
        target_name = prepare_target_name(config)
        self.assertEqual(target_name, "somewhere/download/02/12.nc")

    def test_target_name_date_and_target_directory(self):
        config = {
            'parameters': {
                'partition_keys': ['date'],
                'target_path': 'somewhere',
                'target_filename': '-download.nc',
                'append_date_dirs': 'true',
                'force_download': False
            },
            'selection': {
                'date': ['2017-01-15'],
            }
        }
        target_name = prepare_target_name(config)
        self.assertEqual(target_name, "somewhere/2017/01/15-download.nc")

    def test_target_name_date_and_target_directory_additional_partitions(self):
        config = {
            'parameters': {
                'partition_keys': ['date', 'pressure_level'],
                'target_path': 'somewhere',
                'target_filename': '-pressure-{}.nc',
                'append_date_dirs': 'true',
                'force_download': False
            },
            'selection': {
                'features': ['pressure'],
                'pressure_level': ['500'],
                'date': ['2017-01-15'],
            }
        }
        target_name = prepare_target_name(config)
        self.assertEqual(target_name, "somewhere/2017/01/15-pressure-500.nc")

    def test_target_name_date_and_target_directory_additional_partitions_in_path(self):
        config = {
            'parameters': {
                'partition_keys': ['date', 'expver', 'pressure_level'],
                'target_path': 'somewhere/expver-{}',
                'target_filename': '-pressure-{}.nc',
                'append_date_dirs': 'true',
                'force_download': False
            },
            'selection': {
                'features': ['pressure'],
                'pressure_level': ['500'],
                'date': ['2017-01-15'],
                'expver': ['1'],
            }
        }
        target_name = prepare_target_name(config)
        self.assertEqual(target_name, "somewhere/expver-1/2017/01/15-pressure-500.nc")


if __name__ == '__main__':
    unittest.main()
