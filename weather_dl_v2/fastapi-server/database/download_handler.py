import abc
from firebase_admin import firestore
from google.cloud.firestore_v1 import DocumentSnapshot
from google.cloud.firestore_v1.types import WriteResult
from database.session import get_db

def get_download_handler():
    return DownloadHandlerFirestore(db=get_db())

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

class DownloadHandlerFirestore(DownloadHandler):
    def __init__(self, db: firestore.firestore.Client):
        self.db = db
        self.collection = "download"

    def _start_download(self, config_name: str, client_name: str) -> None:
        result: WriteResult = self.db.collection('download').document(config_name).set(
            {'config_name': config_name, 'client_name': client_name}
            )

        print(f"Added {config_name} in 'download' collection. Update_time: {result.update_time}.")

    def _stop_download(self, config_name: str) -> None:
        timestamp = self.db.collection('download').document(config_name).delete()
        print(f"Removed {config_name} in 'download' collection. Update_time: {timestamp}.")

    def _check_download_exists(self, config_name: str) -> bool:
        result: DocumentSnapshot = self.db.collection('download').document(config_name).get()
        return result.exists