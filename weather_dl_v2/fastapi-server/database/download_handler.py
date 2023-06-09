import abc
import logging
from firebase_admin import firestore
from google.cloud.firestore_v1 import DocumentSnapshot
from google.cloud.firestore_v1.types import WriteResult
from database.session import get_db

logger = logging.getLogger(__name__)

def get_download_handler():
    return DownloadHandlerFirestore(db=get_db())

def get_mock_download_handler():
    return DownloadHandlerMock()

class DownloadHandler(abc.ABC):
    @abc.abstractmethod
    def _start_download(self, config_name: str, client_name: str) -> None:
        pass

    @abc.abstractmethod
    def _stop_download(self, config_name: str) -> None:
        pass

    @abc.abstractmethod
    def _check_download_exists(self, config_name: str) -> bool:
        pass

class DownloadHandlerMock(DownloadHandler):
    def __init__(self):
        pass

    def _start_download(self, config_name: str, client_name: str) -> None:
        logger.info(f"Added {config_name} in 'download' collection. Update_time: 000000.")

    def _stop_download(self, config_name: str) -> None:
        logger.info(f"Removed {config_name} in 'download' collection. Update_time: 000000.")

    def _check_download_exists(self, config_name: str) -> bool:
        if config_name == "no_exist":
            return False
        else:
            return True

class DownloadHandlerFirestore(DownloadHandler):
    def __init__(self, db: firestore.firestore.Client):
        self.db = db
        self.collection = "download"

    def _start_download(self, config_name: str, client_name: str) -> None:
        result: WriteResult = self.db.collection('download').document(config_name).set(
            {'config_name': config_name, 'client_name': client_name}
            )

        logger.info(f"Added {config_name} in 'download' collection. Update_time: {result.update_time}.")

    def _stop_download(self, config_name: str) -> None:
        timestamp = self.db.collection('download').document(config_name).delete()
        logger.info(f"Removed {config_name} in 'download' collection. Update_time: {timestamp}.")

    def _check_download_exists(self, config_name: str) -> bool:
        result: DocumentSnapshot = self.db.collection('download').document(config_name).get()
        return result.exists