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


import abc
import logging
import json
import typing as t
from app.services.network_service import network_service
from app.cli_config import get_config

logger = logging.getLogger(__name__)


class DownloadService(abc.ABC):

    @abc.abstractmethod
    def _list_all_downloads(self):
        pass

    @abc.abstractmethod
    def _list_all_downloads_by_filter(self, filter_dict: dict):
        pass

    @abc.abstractmethod
    def _get_download_by_config(self, config_name: str):
        pass
    
    @abc.abstractmethod
    def _show_config_content(self, config_name: str):
        pass

    @abc.abstractmethod
    def _add_new_download(
        self, file_path: str, licenses: t.List[str], force_download: bool
    ):
        pass

    @abc.abstractmethod
    def _remove_download(self, config_name: str):
        pass

    @abc.abstractmethod
    def _refetch_config_partitions(self, config_name: str, licenses: t.List[str]):
        pass


class DownloadServiceNetwork(DownloadService):

    def __init__(self):
        self.endpoint = f"{get_config().BASE_URI}/download"

    def _list_all_downloads(self):
        return network_service.get(
            uri=self.endpoint, header={"accept": "application/json"}
        )

    def _list_all_downloads_by_filter(self, filter_dict: dict):
        return network_service.get(
            uri=self.endpoint,
            header={"accept": "application/json"},
            query=filter_dict,
        )

    def _get_download_by_config(self, config_name: str):
        return network_service.get(
            uri=f"{self.endpoint}/{config_name}",
            header={"accept": "application/json"},
        )
    
    def _show_config_content(self, config_name: str):
        return network_service.get(
            uri=f"{self.endpoint}/show/{config_name}",
            header={"accept": "application/json"},
        )

    def _add_new_download(
        self, file_path: str, licenses: t.List[str], force_download: bool
    ):
        try:
            file = {"file": open(file_path, "rb")}
        except FileNotFoundError:
            return "File not found."

        return network_service.post(
            uri=self.endpoint,
            header={"accept": "application/json"},
            file=file,
            payload={"licenses": licenses},
            query={"force_download": force_download},
        )

    def _remove_download(self, config_name: str):
        return network_service.delete(
            uri=f"{self.endpoint}/{config_name}", header={"accept": "application/json"}
        )

    def _refetch_config_partitions(self, config_name: str, licenses: t.List[str]):
        return network_service.post(
            uri=f"{self.endpoint}/retry/{config_name}",
            header={"accept": "application/json"},
            payload=json.dumps({"licenses": licenses}),
        )


class DownloadServiceMock(DownloadService):
    pass


def get_download_service(test: bool = False):
    if test:
        return DownloadServiceMock()
    else:
        return DownloadServiceNetwork()


download_service = get_download_service()
