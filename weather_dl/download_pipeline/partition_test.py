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

from .manifest import MockManifest, Location, DownloadStatus, LocalManifest, Status, Stage
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

    def create_partition_configs(self, configs,
                                 store: t.Optional[Store] = None,
                                 schedule='in-order',
                                 n_requests_per: int = 1) -> t.List[Config]:
        subsections = get_subsections(configs[0])
        params_cycle = itertools.cycle(subsections)

        return (EagerPipeline()
                | beam.Create(configs)
                | PartitionConfig(store, params_cycle, self.dummy_manifest, schedule,
                                  len(subsections) * n_requests_per))

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
        actual = self.create_partition_configs([config_obj])

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
        actual = self.create_partition_configs([config_obj])

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
        actual = self.create_partition_configs([config_obj])

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
        actual = self.create_partition_configs([config_obj])

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

            self.create_partition_configs([config_obj])

            with open(self.dummy_manifest.location, 'r') as f:
                actual = json.load(f)

            self.assertListEqual(
                [d['selection'] for d in actual.values()], [
                    json.dumps({**config['selection'], **{'year': [str(i)]}})
                    for i in range(2015, 2021)
                ])

            self.assertTrue(
                all([d['status'] == 'scheduled' for d in actual.values()])
            )

    def test_prepare_partition_records_download_status_to_manifest_for_already_downloaded_shard(self):
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
            self.mock_store = InMemoryStore()
            self.mock_store.open('download-2015.nc')
            self.dummy_manifest = LocalManifest(Location(tmpdir))

            self.create_partition_configs(configs=[config_obj], store=self.mock_store)

            with open(self.dummy_manifest.location, 'r') as f:
                actual = json.load(f)

            self.assertListEqual(
                [d['selection'] for d in actual.values()], [
                    json.dumps({**config['selection'], **{'year': [str(i)]}})
                    for i in range(2015, 2021)
                ])

            self.assertTrue(
                all([d['status'] == 'scheduled' if d['location'] != 'download-2015.nc'
                     else d['stage'] == 'upload' and d['status'] == 'success'
                     for d in actual.values()])
            )

    def test_prepare_partition_update_download_status_for_downloaded_shard_missing_upload_entry(self):
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
            self.mock_store = InMemoryStore()
            self.mock_store.open('download-2015.nc')
            self.dummy_manifest = LocalManifest(Location(tmpdir))
            download_status = DownloadStatus(selection={**config['selection'], **{'year': [2015]}},
                                             location='download-2015.nc',
                                             status=Status.SUCCESS,
                                             stage=Stage.DOWNLOAD)
            self.dummy_manifest._update(download_status)

            self.create_partition_configs(configs=[config_obj], store=self.mock_store)

            with open(self.dummy_manifest.location, 'r') as f:
                actual = json.load(f)

            self.assertListEqual(
                [d['selection'] for d in actual.values()], [
                    json.dumps({**config['selection'], **{'year': [str(i)]}})
                    for i in range(2015, 2021)
                ])

            self.assertTrue(
                all([d['status'] == 'scheduled' if d['location'] != 'download-2015.nc'
                     else d['stage'] == 'upload' and d['status'] == 'success'
                     for d in actual.values()])
            )

    def test_prepare_partition_update_manifest_for_failed_upload_status_of_downloaded_shard(self):
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
            self.mock_store = InMemoryStore()
            self.mock_store.open('download-2015.nc')
            self.dummy_manifest = LocalManifest(Location(tmpdir))
            download_status = DownloadStatus(selection={**config['selection'], **{'year': [2015]}},
                                             location='download-2015.nc',
                                             status=Status.FAILURE,
                                             error='error',
                                             stage=Stage.UPLOAD)
            self.dummy_manifest._update(download_status)

            self.create_partition_configs(configs=[config_obj], store=self.mock_store)

            with open(self.dummy_manifest.location, 'r') as f:
                actual = json.load(f)

            self.assertListEqual(
                [d['selection'] for d in actual.values()], [
                    json.dumps({**config['selection'], **{'year': [str(i)]}})
                    for i in range(2015, 2021)
                ])

            self.assertTrue(
                all([d['status'] == 'scheduled' if d['location'] != 'download-2015.nc'
                     else d['stage'] == 'upload' and d['status'] == 'success'
                     for d in actual.values()])
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
        actual = self.create_partition_configs([config_obj], store=skip_odd_files)
        research_configs = [cfg for cfg in actual if cfg and t.cast('str', cfg.kwargs.get('api_url', "")).endswith('1')]
        cloud_configs = [cfg for cfg in actual if cfg and t.cast('str', cfg.kwargs.get('api_url', "")).endswith('2')]

        self.assertEqual(len(research_configs), len(cloud_configs))

    def test_multi_config_partition_single_key(self):
        configs = [
            Config.from_dict({
                'parameters': {
                    'partition_keys': ['year'],
                    'target_path': 'download-{}-%d.nc' % level,
                },
                'selection': {
                    'features': ['pressure', 'temperature', 'wind_speed_U', 'wind_speed_V'],
                    'month': [str(i) for i in range(1, 13)],
                    'year': [str(i) for i in range(2015, 2021)],
                    'level': [str(level)]
                }
            }) for level in range(500, 901, 100)
        ]

        actual = self.create_partition_configs(configs)

        self.assertListEqual([d.selection for d in actual], [
            {**configs[0].selection, **{'year': [str(i)], 'level': [str(level)]}}
            for level in range(500, 901, 100)
            for i in range(2015, 2021)
        ])

    def test_multi_config_partition_single_key_fair_schedule(self):
        configs = [
            Config.from_dict({
                'parameters': {
                    'partition_keys': ['year'],
                    'target_path': 'download-{}-%d.nc' % level,
                },
                'selection': {
                    'features': ['pressure', 'temperature', 'wind_speed_U', 'wind_speed_V'],
                    'month': [str(i) for i in range(1, 13)],
                    'year': [str(i) for i in range(2015, 2021)],
                    'level': [str(level)]
                }
            }) for level in range(500, 901, 100)
        ]

        actual = self.create_partition_configs(configs, schedule='fair')

        self.assertListEqual([d.selection for d in actual], [
            {**configs[0].selection, **{'year': [str(i)], 'level': [str(level)]}}
            for i in range(2015, 2021)
            for level in range(500, 901, 100)
        ])

    def test_multi_config_partition_multi_params_multi_key(self):
        config_dicts = [
            {
                'parameters': dict(
                    partition_keys=['year', 'month'],
                    target_path='download-{}-{}-%d.nc' % level,
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
                    'year': [str(i) for i in range(2015, 2017)],
                    'level': [str(level)]
                }
            } for level in range(500, 901, 100)
        ]

        actual = self.create_partition_configs([Config.from_dict(c) for c in config_dicts])

        combinations = [
            {
                'parameters': config['parameters'],
                'selection': {**config['selection'], **{'year': [str(year)], 'month': [str(month)]}}
            }
            for config in config_dicts
            for year in range(2015, 2017)
            for month in range(1, 3)
        ]

        subsections = get_subsections(Config.from_dict(config_dicts[0]))
        expected = []
        for config, (name, params) in zip(combinations, itertools.cycle(subsections)):
            config['parameters'].update(params)
            config['parameters']['subsection_name'] = name
            expected.append(Config.from_dict(config))

        self.assertListEqual(actual, expected)

    def test_multi_config_partition_multi_params_fair_schedule(self):
        config_dicts = [
            {
                'parameters': dict(
                    partition_keys=['year'],
                    target_path='download-{}-%d.nc' % level,
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
                    'month': [str(1)],
                    'year': [str(i) for i in range(2015, 2017)],
                    'level': [str(level)]
                }
            } for level in range(500, 901, 100)
        ]

        # Three licenses, with one request per license (see groups)
        actual = self.create_partition_configs([Config.from_dict(c) for c in config_dicts],
                                               schedule='fair', n_requests_per=3)

        combinations = [
            {
                'parameters': config['parameters'],
                'selection': {**config['selection'], **{'year': [str(year)]}}
            }
            for config in config_dicts
            for year in range(2015, 2017)
        ]

        subsections = get_subsections(Config.from_dict(config_dicts[0]))
        expected = []
        for config, (name, params) in zip(combinations, itertools.cycle(subsections)):
            config['parameters'].update(params)
            config['parameters']['subsection_name'] = name
            expected.append(Config.from_dict(config))

        self.assertListEqual(actual, expected)

    def test_multi_config_partition_multi_params_keys_and_requests_with_fair_schedule(self):
        self.maxDiff = None
        config_dicts = [
            {
                'parameters': dict(
                    partition_keys=['year', 'month'],
                    target_path='download-{}-{}-%d.nc' % level,
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
                    'year': [str(i) for i in range(2015, 2017)],
                    'level': [str(level)]
                }
            } for level in range(500, 901, 100)
        ]

        # Three licenses, with two requests per license (see groups)
        actual = self.create_partition_configs([Config.from_dict(c) for c in config_dicts],
                                               schedule='fair', n_requests_per=3 * 2)

        combinations = [
            {
                'parameters': config['parameters'],
                'selection': {**config['selection'], **{'year': [str(year)], 'month': [str(month)]}}
            }
            for config in config_dicts
            for year in range(2015, 2017)
            for month in range(1, 3)
        ]

        subsections = get_subsections(Config.from_dict(config_dicts[0]))
        expected = []
        for config, (name, params) in zip(combinations, itertools.cycle(subsections)):
            config['parameters'].update(params)
            config['parameters']['subsection_name'] = name
            expected.append(Config.from_dict(config))

        self.assertListEqual(actual, expected)

    def test_hdate_partition_single_key(self):
        config = Config.from_dict({
                'parameters': {
                    'partition_keys': ['date'],
                    'target_path': 'download-{}.nc',
                },
                'selection': {
                    'features': ['pressure', 'temperature', 'wind_speed_U', 'wind_speed_V'],
                    'date': ['2016-01-04', '2016-01-07', '2016-01-11'],
                    'hdate': ['1', '2', '3'],
                }
            })

        actual = self.create_partition_configs([config])

        expected = [{'date': ['2016-01-04'], 'hdate': ['2015-01-04', '2014-01-04', '2013-01-04']},
                    {'date': ['2016-01-07'], 'hdate': ['2015-01-07', '2014-01-07', '2013-01-07']},
                    {'date': ['2016-01-11'], 'hdate': ['2015-01-11', '2014-01-11', '2013-01-11']},
                    ]
        self.assertListEqual([d.selection for d in actual], [{**config.selection, **e} for e in expected])


class SkipPartitionsTest(unittest.TestCase):

    def setUp(self) -> None:
        self.mock_store = InMemoryStore()
        self.dummy_manifest = MockManifest(Location('mock://dummy'))

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
        actual = skip_partition(config_obj, self.mock_store, self.dummy_manifest)

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
        actual = skip_partition(config_obj, self.mock_store, self.dummy_manifest)

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

        actual = skip_partition(config_obj, self.mock_store, self.dummy_manifest)

        self.assertEqual(actual, True)


if __name__ == '__main__':
    unittest.main()
