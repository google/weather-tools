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

import os
import unittest

from .pipeline import run
import weather_dl


class ConfigTest(unittest.TestCase):

    def setUp(self):
        self.data_dir = f'{next(iter(weather_dl.__path__))}/../configs'

    def test_process_config_files(self):
        for filename in os.listdir(self.data_dir):
            with self.subTest(filename=filename):
                config = os.path.join(self.data_dir, filename)
                try:
                    run(["weather-dl", config, "--dry-run"])
                except:  # noqa: E722
                    self.fail(f'Config {filename!r} is incorrect.')


if __name__ == '__main__':
    unittest.main()
