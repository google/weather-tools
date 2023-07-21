import abc
import logging
import typing as t
from app.services.network_service import network_service
from app.config import Config

logger = logging.getLogger(__name__)

class DownloadService(abc.ABC):

    @abc.abstractmethod
    def _list_all_downloads(self):
        pass

    @abc.abstractmethod
    def _list_all_downloads_by_client_name(self, client_name: str):
        pass

    @abc.abstractmethod
    def _get_download_by_config(self, config_name: str):
        pass

    @abc.abstractmethod
    def _add_new_download(self, file_path: str, licenses: t.List[str]):
        pass

    @abc.abstractmethod
    def _remove_download(self, config_name: str):
        pass

class DownloadServiceNetwork(DownloadService):
    def __init__(self):
        self.endpoint = f"{Config().BASE_URI}/download"

    def _list_all_downloads(self):
        return network_service.get(
            uri = self.endpoint,
            header = {"accept": "application/json"}
        )

    def _list_all_downloads_by_client_name(self, client_name: str):
        return network_service.get(
            uri = self.endpoint,
            header = {"accept": "application/json"},
            query = {"client_name": client_name}
        )

    def _get_download_by_config(self, config_name: str):
        return network_service.get(
            uri = f"{self.endpoint}/download{config_name}",
            header = {"accept": "application/json"}
        )

    def _add_new_download(self, file_path: str, licenses: t.List[str]):
        try:
            file = {"file" : open(file_path, 'rb')}
        except FileNotFoundError:
            return "File not found."

        return network_service.post(
            uri=self.endpoint,
            header = {"accept": "application/json"},
            file = file,
            payload = {"licenses": licenses}
        )

    def _remove_download(self, config_name: str):
        return network_service.delete(
            uri=f"{self.endpoint}/{config_name}",
            header = {"accept": "application/json"}
        )

class DownloadServiceMock(DownloadService):
    pass

def get_download_service(test: bool = False):
    if test:
        return DownloadServiceMock()
    else:
        return DownloadServiceNetwork()

download_service = get_download_service()
