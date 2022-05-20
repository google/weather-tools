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
import itertools
import json
import tempfile
import typing as t
import unittest
from unittest.mock import MagicMock

import apache_beam as beam
from xarray_beam._src.test_util import EagerPipeline

from .manifest import MockManifest, Location, LocalManifest
from .parsers import get_subsections
from .partition import skip_partition, PartitionConfig
from .stores import InMemoryStore, Store
from .config import Config


class OddFilesDoNotExistStore(InMemoryStore):
    def __init__(self):
        super().__init__()
        self.count = 0

    def exists(self, filename: str) -> bool:
        ret = self.count % 2 == 0
        self.count += 1
        return ret


class PreparePartitionTest(unittest.TestCase):

    def setUp(self) -> None:
        self.dummy_manifest = MockManifest(Location('mock://dummy'))

    def create_partition_configs(self, config, store: t.Optional[Store] = None) -> t.List[Config]:
        subsections = get_subsections(config)
        params_cycle = itertools.cycle(subsections)

        return (EagerPipeline()
                | beam.Create([config])
                | PartitionConfig(store, params_cycle, self.dummy_manifest))

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

        config_obj = Config.from_dict(config)
        actual = self.create_partition_configs(config_obj)

        self.assertListEqual([d.selection for d in actual], [
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

        config_obj = Config.from_dict(config)
        actual = self.create_partition_configs(config_obj)

        self.assertListEqual([d.selection for d in actual], [
            {**config['selection'], **{'year': ['2015'], 'month': ['1']}},
            {**config['selection'], **{'year': ['2015'], 'month': ['2']}},
            {**config['selection'], **{'year': ['2016'], 'month': ['1']}},
            {**config['selection'], **{'year': ['2016'], 'month': ['2']}},
        ])

    def test_partition_multi_key_single_values(self):
        config = {
            'parameters': {
                'partition_keys': ['year', 'month'],
                'target_path': 'download-{}-{}.nc',
            },
            'selection': {
                'features': ['pressure', 'temperature', 'wind_speed_U', 'wind_speed_V'],
                'month': ['1'],
                'year': ['2015'],
            }
        }

        config_obj = Config.from_dict(config)
        actual = self.create_partition_configs(config_obj)

        self.assertListEqual([d.selection for d in actual], [
            {**config['selection'], **{'year': ['2015'], 'month': ['1']}},
        ])

    def test_partition_multi_params_multi_key(self):
        config = {
            'parameters': dict(
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

        config_obj = Config.from_dict(config)
        actual = self.create_partition_configs(config_obj)

        expected = [Config.from_dict(it) for it in [
                {'parameters': dict(config['parameters'], api_key='KKKK1', api_url='UUUU1', subsection_name='research'),
                 'selection': {**config['selection'],
                               **{'year': ['2015'], 'month': ['1']}}},
                {'parameters': dict(config['parameters'], api_key='KKKK2', api_url='UUUU2', subsection_name='cloud'),
                 'selection': {**config['selection'],
                               **{'year': ['2015'], 'month': ['2']}}},
                {'parameters': dict(config['parameters'], api_key='KKKK3', api_url='UUUU3', subsection_name='deepmind'),
                 'selection': {**config['selection'],
                               **{'year': ['2016'], 'month': ['1']}}},
                {'parameters': dict(config['parameters'], api_key='KKKK1', api_url='UUUU1', subsection_name='research'),
                 'selection': {**config['selection'],
                               **{'year': ['2016'], 'month': ['2']}}},
        ]]

        self.assertListEqual(actual, expected)

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

        config_obj = Config.from_dict(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            self.dummy_manifest = LocalManifest(Location(tmpdir))

            self.create_partition_configs(config_obj)

            with open(self.dummy_manifest.location, 'r') as f:
                actual = json.load(f)

            self.assertListEqual(
                [d['selection'] for d in actual.values()], [
                    {**config['selection'], **{'year': [str(i)]}}
                    for i in range(2015, 2021)
                ])

            self.assertTrue(
                all([d['status'] == 'scheduled' for d in actual.values()])
            )

    def test_skip_partitions__never_unbalances_licenses(self):
        skip_odd_files = OddFilesDoNotExistStore()
        config = {
            'parameters': dict(
                partition_keys=['year', 'month'],
                target_path='download-{}-{}.nc',
                research={
                    'api_key': 'KKKK1',
                    'api_url': 'UUUU1'
                },
                cloud={
                    'api_key': 'KKKK2',
                    'api_url': 'UUUU2'
                }
            ),
            'selection': {
                'features': ['pressure', 'temperature', 'wind_speed_U', 'wind_speed_V'],
                'month': [str(i) for i in range(1, 3)],
                'year': [str(i) for i in range(2016, 2020)]
            }
        }

        config_obj = Config.from_dict(config)
        actual = self.create_partition_configs(config_obj, store=skip_odd_files)
        research_configs = [cfg for cfg in actual if cfg and t.cast('str', cfg.kwargs.get('api_url', "")).endswith('1')]
        cloud_configs = [cfg for cfg in actual if cfg and t.cast('str', cfg.kwargs.get('api_url', "")).endswith('2')]

        self.assertEqual(len(research_configs), len(cloud_configs))


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

        config_obj = Config.from_dict(config)
        actual = skip_partition(config_obj, self.mock_store)

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

        config_obj = Config.from_dict(config)
        actual = skip_partition(config_obj, self.mock_store)

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

        config_obj = Config.from_dict(config)

        self.mock_store.exists = MagicMock(return_value=True)

        actual = skip_partition(config_obj, self.mock_store)

        self.assertEqual(actual, True)


if __name__ == '__main__':
    unittest.main()
