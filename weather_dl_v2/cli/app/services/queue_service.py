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


import abc
import logging
import json
import typing as t
from app.services.network_service import network_service
from app.cli_config import get_config

logger = logging.getLogger(__name__)


class QueueService(abc.ABC):

    @abc.abstractmethod
    def _get_all_license_queues(self):
        pass

    @abc.abstractmethod
    def _get_license_queue_by_client_name(self, client_name: str):
        pass

    @abc.abstractmethod
    def _get_queue_by_license(self, license_id: str):
        pass

    @abc.abstractmethod
    def _edit_license_queue(self, license_id: str, priority_list: t.List[str]):
        pass

    @abc.abstractmethod
    def _edit_config_absolute_priority(
        self, license_id: str, config_name: str, priority: int
    ):
        pass


class QueueServiceNetwork(QueueService):

    def __init__(self):
        self.endpoint = f"{get_config().BASE_URI}/queues"

    def _get_all_license_queues(self):
        return network_service.get(
            uri=self.endpoint, header={"accept": "application/json"}
        )

    def _get_license_queue_by_client_name(self, client_name: str):
        return network_service.get(
            uri=self.endpoint,
            header={"accept": "application/json"},
            query={"client_name": client_name},
        )

    def _get_queue_by_license(self, license_id: str):
        return network_service.get(
            uri=f"{self.endpoint}/{license_id}", header={"accept": "application/json"}
        )

    def _edit_license_queue(self, license_id: str, priority_list: t.List[str]):
        return network_service.post(
            uri=f"{self.endpoint}/{license_id}",
            header={"accept": "application/json", "Content-Type": "application/json"},
            payload=json.dumps(priority_list),
        )

    def _edit_config_absolute_priority(
        self, license_id: str, config_name: str, priority: int
    ):
        return network_service.put(
            uri=f"{self.endpoint}/priority/{license_id}",
            header={"accept": "application/json"},
            query={"config_name": config_name, "priority": priority},
        )


class QueueServiceMock(QueueService):
    pass


def get_queue_service(test: bool = False):
    if test:
        return QueueServiceMock()
    else:
        return QueueServiceNetwork()


queue_service = get_queue_service()
