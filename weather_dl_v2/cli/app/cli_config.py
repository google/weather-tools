# Copyright 2023 Google LLC
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

import pkg_resources
import dataclasses
import typing as t
import json
import os

Values = t.Union[t.List["Values"], t.Dict[str, "Values"], bool, int, float, str]  # pytype: disable=not-supported-yet


@dataclasses.dataclass
class CliConfig:
    pod_ip: str = ""
    port: str = ""

    @property
    def BASE_URI(self) -> str:
        return f"http://{self.pod_ip}:{self.port}"

    kwargs: t.Optional[t.Dict[str, Values]] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_dict(cls, config: t.Dict):
        config_instance = cls()

        for key, value in config.items():
            if hasattr(config_instance, key):
                setattr(config_instance, key, value)
            else:
                config_instance.kwargs[key] = value

        return config_instance


cli_config = None


def get_config():
    global cli_config

    cli_config_json = pkg_resources.resource_filename('app', 'data/cli_config.json')

    if cli_config is None:
        with open(cli_config_json) as file:
            firestore_dict = json.load(file)
            cli_config = CliConfig.from_dict(firestore_dict)

    return cli_config
