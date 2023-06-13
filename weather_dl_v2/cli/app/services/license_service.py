import abc
import logging
import json
import typing as t
from app.services.network_service import network_service
from app.config import Config

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


class LicneseServiceNetwork(LicenseService):
    def __init__(self):
        self.endpoint = f"{Config().BASE_URI}/license"

    def _get_all_license(self):
        return network_service.get(
            uri = self.endpoint,
            header = {"accept": "application/json"}
        )
    
    def _get_all_license_by_client_name(self, client_name: str):
        return network_service.get(
            uri = self.endpoint,
            header = {"accept": "application/json"},
            query = {"client_name": client_name}
        )
    
    def _get_license_by_license_id(self, license_id: str):
        return network_service.get(
            uri = f"{self.endpoint}/{license_id}",
            header = {"accept": "application/json"},
        )
    
    def _add_license(self, license_dict: dict):
        return network_service.post(
            uri = self.endpoint,
            header = {"accept": "application/json"},
            payload = json.dumps(license_dict)
        )
    
    def _remove_license(self, license_id: str):
        return network_service.delete(
            uri = f"{self.endpoint}/{license_id}",
            header = {"accept": "application/json"},
        )
    
    def _update_license(self, license_id: str, license_dict: dict):
        return network_service.put(
            uri = f"{self.endpoint}/{license_id}",
            header = {"accept": "application/json"},
            payload = json.dumps(license_dict)
        )

    
class LicenseServiceMock(LicenseService):
    pass

def get_license_service(test: bool = False):
    if test:
        return LicenseServiceMock()
    else:
        return LicneseServiceNetwork()
    
license_service = get_license_service()
