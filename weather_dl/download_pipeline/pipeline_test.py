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
import argparse
import copy
import dataclasses
import getpass
import os
import typing as t
import unittest

from apache_beam.options.pipeline_options import PipelineOptions

import weather_dl
from .manifest import FirestoreManifest, Location, NoOpManifest, LocalManifest
from .pipeline import run, PipelineArgs
from .stores import TempFileStore, LocalFileStore
from .config import Config

PATH_TO_CONFIG = os.path.join(os.path.dirname(list(weather_dl.__path__)[0]), 'configs', 'era5_example_config.cfg')
CONFIG = {
        'parameters': {'client': 'cds',
                       'dataset': 'reanalysis-era5-pressure-levels',
                       'target_path': 'gs://ecmwf-output-test/era5/{year:04d}/{month:02d}/{day:02d}'
                                      '-pressure-{pressure_level}.nc',
                       'partition_keys': ['year', 'month', 'day', 'pressure_level'],
                       'force_download': False,
                       'user_id': getpass.getuser()},
        'selection': {'product_type': 'reanalysis',
                      'format': 'netcdf',
                      'variable': ['divergence', 'fraction_of_cloud_cover', 'geopotential'],
                      'pressure_level': ['500'],
                      'year': ['2015', '2016', '2017'], 'month': ['01'],
                      'day': ['01', '15'],
                      'time': ['00:00', '06:00', '12:00', '18:00']}
    }
DEFAULT_ARGS = PipelineArgs(
    known_args=argparse.Namespace(config=PATH_TO_CONFIG,
                                  force_download=False,
                                  dry_run=False,
                                  local_run=False,
                                  manifest_location='fs://downloader-manifest',
                                  num_requests_per_key=-1),
    pipeline_options=PipelineOptions('--save_main_session True'.split()),
    config=Config.from_dict(CONFIG),
    client_name='cds',
    store=None,
    manifest=FirestoreManifest(Location('fs://downloader-manifest?projectId=None')),
    num_requesters_per_key=5,
)


def default_args(parameters: t.Optional[t.Dict] = None, selection: t.Optional[t.Dict] = None,
                 known_args: t.Optional[t.Dict] = None, **kwargs) -> PipelineArgs:
    if parameters is None:
        parameters = {}
    if selection is None:
        selection = {}
    if known_args is None:
        known_args = {}
    args = dataclasses.replace(DEFAULT_ARGS, **kwargs)
    temp_config = copy.deepcopy(CONFIG)
    temp_config['parameters'].update(parameters)
    temp_config['selection'].update(selection)
    args.config = Config.from_dict(temp_config)
    args.known_args = copy.deepcopy(args.known_args)
    for k, v in known_args.items():
        setattr(args.known_args, k, v)
    return args


class ParsePipelineArgs(unittest.TestCase):
    DEFAULT_CMD = f'weather-dl {PATH_TO_CONFIG}'

    TEST_CASES = [
        ('happy path', DEFAULT_CMD, DEFAULT_ARGS),
        ('force download', f'{DEFAULT_CMD} -f', default_args(
            dict(force_download=True), known_args=dict(force_download=True))
         ),
        ('dry run', f'{DEFAULT_CMD} -d', default_args(
            dict(force_download=True), known_args=dict(dry_run=True), client_name='fake',
            store=TempFileStore('dry_run'), manifest=NoOpManifest(Location('noop://dry-run')),
            num_requesters_per_key=1)
         ),
        ('local run', f'{DEFAULT_CMD} -l', default_args(
            known_args=dict(local_run=True), store=LocalFileStore(f'{os.getcwd()}/local_run'),
            manifest=LocalManifest(Location(f'{os.getcwd()}/local_run')),
            pipeline_options=PipelineOptions('--runner DirectRunner --save_main_session True'.split()))
         ),
        ('user-specified number of requests per key', f'{DEFAULT_CMD} -n 7', default_args(
            known_args=dict(num_requests_per_key=7), num_requesters_per_key=7)
         ),
    ]

    def test_run(self):
        for msg, args, expected in self.TEST_CASES:
            with self.subTest(msg):
                actual = run(args.split())
                self.assertEqual(vars(actual.known_args), vars(expected.known_args))
                self.assertEqual(
                    actual.pipeline_options.get_all_options(drop_default=True),
                    expected.pipeline_options.get_all_options(drop_default=True)
                )
                self.assertEqual(actual.config, expected.config)
                self.assertEqual(actual.client_name, expected.client_name)
                self.assertEqual(type(actual.store), type(expected.store))
                self.assertEqual(actual.manifest, expected.manifest)
                self.assertEqual(type(actual.manifest), type(expected.manifest))
                self.assertEqual(actual.num_requesters_per_key, expected.num_requesters_per_key)


if __name__ == '__main__':
    unittest.main()
