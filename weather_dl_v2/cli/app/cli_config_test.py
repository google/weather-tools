# Copyright 2026 Google LLC
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

import importlib
import sys
import unittest
from unittest import mock


class CliConfigTest(unittest.TestCase):
    def test_get_config_works_without_pkg_resources(self):
        # setuptools >= 82 no longer ships `pkg_resources`; the CLI must not need it.
        with mock.patch.dict(sys.modules, {"pkg_resources": None}):
            for name in ("app.cli_config", "app.subcommands.config"):
                sys.modules.pop(name, None)
            cli_config = importlib.import_module("app.cli_config")
            importlib.import_module("app.subcommands.config")
            cli_config.cli_config = None
            config = cli_config.get_config()
        self.assertTrue(config.pod_ip)
        self.assertTrue(config.port)


if __name__ == "__main__":
    unittest.main()
