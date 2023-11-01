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
from app.services.network_service import network_service
from app.cli_config import get_config

logger = logging.getLogger(__name__)


class LicenseService(abc.ABC):

    @abc.abstractmethod
    def _get_all_license(self):
        pass

    @abc.abstractmethod
    def _get_all_license_by_client_name(self, client_name: str):
        pass

    @abc.abstractmethod
    def _get_license_by_license_id(self, license_id: str):
        pass

    @abc.abstractmethod
    def _add_license(self, license_dict: dict):
        pass

    @abc.abstractmethod
    def _remove_license(self, license_id: str):
        pass

    @abc.abstractmethod
    def _update_license(self, license_id: str, license_dict: dict):
        pass


class LicenseServiceNetwork(LicenseService):

    def __init__(self):
        self.endpoint = f"{get_config().BASE_URI}/license"

    def _get_all_license(self):
        return network_service.get(
            uri=self.endpoint, header={"accept": "application/json"}
        )

    def _get_all_license_by_client_name(self, client_name: str):
        return network_service.get(
            uri=self.endpoint,
            header={"accept": "application/json"},
            query={"client_name": client_name},
        )

    def _get_license_by_license_id(self, license_id: str):
        return network_service.get(
            uri=f"{self.endpoint}/{license_id}",
            header={"accept": "application/json"},
        )

    def _add_license(self, license_dict: dict):
        return network_service.post(
            uri=self.endpoint,
            header={"accept": "application/json"},
            payload=json.dumps(license_dict),
        )

    def _remove_license(self, license_id: str):
        return network_service.delete(
            uri=f"{self.endpoint}/{license_id}",
            header={"accept": "application/json"},
        )

    def _update_license(self, license_id: str, license_dict: dict):
        return network_service.put(
            uri=f"{self.endpoint}/{license_id}",
            header={"accept": "application/json"},
            payload=json.dumps(license_dict),
        )


class LicenseServiceMock(LicenseService):
    pass


def get_license_service(test: bool = False):
    if test:
        return LicenseServiceMock()
    else:
        return LicenseServiceNetwork()


license_service = get_license_service()
