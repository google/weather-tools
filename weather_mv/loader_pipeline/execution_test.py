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
import unittest
import weather_mv

from .pipeline import run, pipeline


class ExecutionTest(unittest.TestCase):

    TEST_DATA_FOLDER = f'{next(iter(weather_mv.__path__))}/test_data'
    DEFAULT_CMD = 'weather-mv --output_table myproject.mydataset.mytable --temp_location "gs://mybucket/tmp" --dry-run'
    LOCAL_DATA_SOURCE = f'{TEST_DATA_FOLDER}/test_data_single_point.nc'
    TEST_CASES = [
        ('local data source and local execution', f'{DEFAULT_CMD} --uris {LOCAL_DATA_SOURCE} --direct_num_workers 2'),
    ]

    def test_run(self):
        for msg, args in self.TEST_CASES:
            with self.subTest(msg):
                pipeline(*run(args.split()))


if __name__ == '__main__':
    unittest.main()
