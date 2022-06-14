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
import json
import unittest

from .pipeline import run


class TestCLI(unittest.TestCase):

    def setUp(self) -> None:
        self.base_cli_args = (
            'weather-mv '
            '-i weather_mv/test_data/test_data_2018*.nc '
            '-o myproject.mydataset.mytable '
            '--import_time 2022-02-04T22:22:12.125893'
        ).split()
        self.tif_base_cli_args = (
            'weather-mv '
            '-i weather_mv/test_data/test_data_tif_start_time.tif '
            '-o myproject.mydataset.mytable '
            '--import_time 2022-02-04T22:22:12.125893'
        ).split()
        self.base_cli_known_args = {
            'uris': 'weather_mv/test_data/test_data_2018*.nc',
            'output_table': 'myproject.mydataset.mytable',
            'dry_run': False,
            'skip_region_validation': False,
            'import_time': '2022-02-04T22:22:12.125893',
            'infer_schema': False,
            'num_shards': 5,
            'topic': None,
            'variables': [],
            'window_size': 1.0,
            'xarray_open_dataset_kwargs': {},
            'coordinate_chunk_size': 10_000,
            'disable_in_memory_copy': False,
            'tif_metadata_for_datetime': None,
        }

    def test_dry_runs_are_allowed(self):
        known_args, _ = run(self.base_cli_args + '--dry-run'.split())
        self.assertEqual(known_args.dry_run, True)

    def test_tif_metadata_for_datetime_raise_error_for_non_tif_file(self):
        with self.assertRaisesRegex(RuntimeError, 'can be specified only for tif files.'):
            run(self.base_cli_args + '--tif_metadata_for_datetime start_time'.split())

    def test_tif_metadata_for_datetime_raise_error_if_flag_is_absent(self):
        with self.assertRaisesRegex(RuntimeError, 'is required for tif files.'):
            run(self.tif_base_cli_args)

    def test_area_only_allows_four(self):
        with self.assertRaisesRegex(AssertionError, 'Must specify exactly 4 lat/long .* N, W, S, E'):
            run(self.base_cli_args + '--area 1 2 3'.split())

        with self.assertRaisesRegex(AssertionError, 'Must specify exactly 4 lat/long .* N, W, S, E'):
            run(self.base_cli_args + '--area 1 2 3 4 5'.split())

        known_args, pipeline_args = run(self.base_cli_args + '--area 1 2 3 4'.split())
        self.assertEqual(pipeline_args, [])
        self.assertEqual(vars(known_args), {
            **self.base_cli_known_args,
            'area': [1, 2, 3, 4]
        })

    def test_topic_creates_a_streaming_pipeline(self):
        _, pipeline_args = run(self.base_cli_args + '--topic projects/myproject/topics/my-topic'.split())
        self.assertEqual(pipeline_args, ['--streaming', 'true'])

    def test_accepts_json_string_for_xarray_open(self):
        xarray_kwargs = dict(engine='cfgrib', backend_kwargs={'filter_by_keys': {'edition': 1}})
        json_kwargs = json.dumps(xarray_kwargs)
        known_args, _ = run(
            self.base_cli_args + ["--xarray_open_dataset_kwargs", f"{json_kwargs}"]
        )
        self.assertEqual(known_args.xarray_open_dataset_kwargs, xarray_kwargs)


if __name__ == '__main__':
    unittest.main()
