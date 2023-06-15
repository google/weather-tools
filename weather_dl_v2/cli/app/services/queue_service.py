import abc
import logging
import json
import typing as t
from app.services.network_service import network_service
from app.config import Config

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

class QueueServiceNetwork(QueueService):
    def __init__(self):
        self.endpoint = f"{Config().BASE_URI}/queues"

    def _get_all_license_queues(self):
        return network_service.get(
            uri = self.endpoint,
            header = {"accept": "application/json"}
        )
    
    def _get_license_queue_by_client_name(self, client_name: str):
        return network_service.get(
            uri = self.endpoint,
            header = {"accept": "application/json"},
            query = {"client_name": client_name}
        )
    
    def _get_queue_by_license(self, license_id: str):
        return network_service.get(
            uri = f"{self.endpoint}/{license_id}",
            header = {"accept": "application/json"}
        )
    
    def _edit_license_queue(self, license_id: str, priority_list: t.List[str]):
        return network_service.post(
            uri = f"{self.endpoint}/{license_id}",
            header = {
                "accept": "application/json",
                'Content-Type': 'application/json'
            },
            payload = json.dumps(priority_list)
        )

class QueueServiceMock(QueueService):
    pass


def get_queue_service(test: bool = False):
    if test:
        return QueueServiceMock()
    else:
        return QueueServiceNetwork()
    
queue_service = get_queue_service()