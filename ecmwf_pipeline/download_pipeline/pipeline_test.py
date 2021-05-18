import io
import unittest
from collections import OrderedDict
from unittest.mock import patch, ANY, MagicMock

from ecmwf_pipeline.download_pipeline.stores import InMemoryStore
from .clients import CdsClient
from .manifest import MockManifest, Location
from .pipeline import fetch_data
from .pipeline import prepare_partition
from .pipeline import skip_partition


class PreparePartitionTest(unittest.TestCase):

    def setUp(self) -> None:
        self.dummy_manifest = MockManifest(Location('mock://dummy'))

    def test_partition_single_key(self):
        config = {
            'parameters': {
                'partition_keys': ['year'],
                'target_template': 'download-{}.nc',
            },
            'selection': {
                'features': ['pressure', 'temperature', 'wind_speed_U', 'wind_speed_V'],
                'month': [str(i) for i in range(1, 13)],
                'year': [str(i) for i in range(2015, 2021)]
            }
        }

        actual = list(prepare_partition(config, manifest=self.dummy_manifest))

        self.assertListEqual([d['selection'] for d in actual], [
            {**config['selection'], **{'year': [str(i)]}}
            for i in range(2015, 2021)
        ])

    def test_partition_multi_key(self):
        config = {
            'parameters': {
                'partition_keys': ['year', 'month'],
                'target_template': 'download-{}-{}.nc',
            },
            'selection': {
                'features': ['pressure', 'temperature', 'wind_speed_U', 'wind_speed_V'],
                'month': [str(i) for i in range(1, 3)],
                'year': [str(i) for i in range(2015, 2017)]
            }
        }

        actual = list(prepare_partition(config, manifest=self.dummy_manifest))

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
                target_template='download-{}-{}.nc',
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

        actual = list(prepare_partition(config, manifest=self.dummy_manifest))

        self.assertListEqual(actual, [
            {'parameters': OrderedDict(config['parameters'], api_key='KKKK1', api_url='UUUU1'),
             'selection': {**config['selection'], **{'year': ['2015'], 'month': ['1']}}},
            {'parameters': OrderedDict(config['parameters'], api_key='KKKK2', api_url='UUUU2'),
             'selection': {**config['selection'], **{'year': ['2015'], 'month': ['2']}}},
            {'parameters': OrderedDict(config['parameters'], api_key='KKKK3', api_url='UUUU3'),
             'selection': {**config['selection'], **{'year': ['2016'], 'month': ['1']}}},
            {'parameters': OrderedDict(config['parameters'], api_key='KKKK1', api_url='UUUU1'),
             'selection': {**config['selection'], **{'year': ['2016'], 'month': ['2']}}},
        ])

    def test_prepare_partition_records_download_status_to_manifest(self):
        config = {
            'parameters': {
                'partition_keys': ['year'],
                'target_template': 'download-{}.nc',
            },
            'selection': {
                'features': ['pressure', 'temperature', 'wind_speed_U', 'wind_speed_V'],
                'month': [str(i) for i in range(1, 13)],
                'year': [str(i) for i in range(2015, 2021)]
            }
        }

        list(prepare_partition(config, manifest=self.dummy_manifest))

        self.assertListEqual([d.selection for d in self.dummy_manifest.records.values()], [
            {**config['selection'], **{'year': [str(i)]}}
            for i in range(2015, 2021)
        ])

        self.assertTrue(
            all([d.status == 'scheduled' for d in self.dummy_manifest.records.values()])
        )


class FetchDataTest(unittest.TestCase):

    def setUp(self) -> None:
        self.dummy_manifest = MockManifest(Location('dummy-manifest'))

    @patch('ecmwf_pipeline.download_pipeline.stores.InMemoryStore.open', return_value=io.StringIO())
    @patch('cdsapi.Client.retrieve')
    def test_fetch_data(self, mock_retrieve, mock_gcs_file):
        config = {
            'parameters': {
                'dataset': 'reanalysis-era5-pressure-levels',
                'partition_keys': ['year', 'month'],
                'target_template': 'gs://weather-dl-unittest/download-{}-{}.nc',
                'api_url': 'https//api-url.com/v1/',
                'api_key': '12345',
            },
            'selection': {
                'features': ['pressure'],
                'month': ['12'],
                'year': ['01']
            }
        }

        fetch_data(config, client=CdsClient(config), manifest=self.dummy_manifest, store=InMemoryStore())

        mock_gcs_file.assert_called_with(
            'gs://weather-dl-unittest/download-01-12.nc',
            'wb'
        )

        mock_retrieve.assert_called_with(
            'reanalysis-era5-pressure-levels',
            config['selection'],
            ANY)

    @patch('ecmwf_pipeline.download_pipeline.stores.InMemoryStore.open', return_value=io.StringIO())
    @patch('cdsapi.Client.retrieve')
    def test_fetch_data__manifest__returns_success(self, mock_retrieve, mock_gcs_file):
        config = {
            'parameters': {
                'dataset': 'reanalysis-era5-pressure-levels',
                'partition_keys': ['year', 'month'],
                'target_template': 'gs://weather-dl-unittest/download-{}-{}.nc',
                'api_url': 'https//api-url.com/v1/',
                'api_key': '12345',
            },
            'selection': {
                'features': ['pressure'],
                'month': ['12'],
                'year': ['01']
            }
        }

        fetch_data(config, client=CdsClient(config), manifest=self.dummy_manifest, store=InMemoryStore())

        self.assertDictContainsSubset(dict(
            selection=config['selection'],
            location='gs://weather-dl-unittest/download-01-12.nc',
            status='success',
            error=None,
            user='unknown',
        ), list(self.dummy_manifest.records.values())[0]._asdict())

    @patch('ecmwf_pipeline.download_pipeline.stores.InMemoryStore.open', return_value=io.StringIO())
    @patch('cdsapi.Client.retrieve')
    def test_fetch_data__manifest__records_retrieve_failure(self, mock_retrieve, mock_gcs_file):
        config = {
            'parameters': {
                'dataset': 'reanalysis-era5-pressure-levels',
                'partition_keys': ['year', 'month'],
                'target_template': 'gs://weather-dl-unittest/download-{}-{}.nc',
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

        fetch_data(config, client=CdsClient(config), manifest=self.dummy_manifest, store=InMemoryStore())

        self.assertDictContainsSubset(dict(
            selection=config['selection'],
            location='gs://weather-dl-unittest/download-01-12.nc',
            status='failure',
            error=str(error),
            user='unknown',
        ), list(self.dummy_manifest.records.values())[0]._asdict())

    @patch('ecmwf_pipeline.download_pipeline.stores.InMemoryStore.open', return_value=io.StringIO())
    @patch('cdsapi.Client.retrieve')
    def test_fetch_data__manifest__records_gcs_failure(self, mock_retrieve, mock_gcs_file):
        config = {
            'parameters': {
                'dataset': 'reanalysis-era5-pressure-levels',
                'partition_keys': ['year', 'month'],
                'target_template': 'gs://weather-dl-unittest/download-{}-{}.nc',
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

        fetch_data(config, client=CdsClient(config), manifest=self.dummy_manifest, store=InMemoryStore())

        self.assertDictContainsSubset(dict(
            selection=config['selection'],
            location='gs://weather-dl-unittest/download-01-12.nc',
            status='failure',
            error=str(error),
            user='unknown',
        ), list(self.dummy_manifest.records.values())[0]._asdict())


class SkipPartitionsTest(unittest.TestCase):

    def setUp(self) -> None:
        self.mock_store = InMemoryStore()

    def test_skip_partition_missing_force_download(self):
        config = {
            'parameters': {
                'partition_keys': ['year', 'month'],
                'target_template': 'download-{}-{}.nc',
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
                'target_template': 'download-{}-{}.nc',
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
                'target_template': 'download-{}-{}.nc',
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


if __name__ == '__main__':
    unittest.main()
