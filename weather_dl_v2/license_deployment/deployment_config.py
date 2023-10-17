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


import dataclasses
import typing as t
import json
import os
import logging

logger = logging.getLogger(__name__)

Values = t.Union[t.List["Values"], t.Dict[str, "Values"], bool, int, float, str]  # pytype: disable=not-supported-yet


@dataclasses.dataclass
class DeploymentConfig:
    download_collection: str = ""
    queues_collection: str = ""
    license_collection: str = ""
    manifest_collection: str = ""
    downloader_k8_image: str = ""
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


deployment_config = None


def get_config():
    global deployment_config
    if deployment_config:
        return deployment_config

    deployment_config_json = "config/config.json"
    if not os.path.exists(deployment_config_json):
        deployment_config_json = os.environ.get('CONFIG_PATH', None)

    if deployment_config_json is None:
        logger.error(f"Couldn't load config file for license deployment.")
        raise FileNotFoundError("Couldn't load config file for license deployment.")

    with open(deployment_config_json) as file:
        config_dict = json.load(file)
        deployment_config = DeploymentConfig.from_dict(config_dict)

    return deployment_config
