import abc
from firebase_admin import firestore
from google.cloud.firestore_v1 import DocumentSnapshot
from google.cloud.firestore_v1.types import WriteResult
from database.session import get_db

def get_queue_handler():
    return QueueHandlerFirestore(db=get_db())

class QueueHandler(abc.ABC):
    @abc.abstractmethod
    def _create_license_queue(self, license_id: str, client_name: str) -> None:
        pass

    @abc.abstractmethod
    def _remove_license_queue(self, license_id: str) -> None:
        pass

    @abc.abstractmethod
    def _get_queues(self) -> list:
        pass

    @abc.abstractmethod
    def _get_queue_by_license_id(self, license_id: str) -> dict:
        pass

    @abc.abstractmethod
    def _get_queue_by_client_name(self, client_name: str) -> list:
        pass

    @abc.abstractmethod
    def _update_license_queue(self, license_id: str, priority_list: list) -> None:
        pass

    @abc.abstractmethod
    def _update_queues_on_start_download(self, config_name: str, licenses: list) -> None:
        pass

    @abc.abstractmethod
    def _update_queues_on_stop_download(self, config_name: str) -> None:
        pass

class QueueHandlerFirestore(QueueHandler):
    def __init__(self, db: firestore.firestore.Client, collection: str = "queues"):
        self.db = db
        self.collection = collection

    def _create_license_queue(self, license_id: str, client_name: str) -> None:
        result: WriteResult = self.db.collection(self.collection).document(license_id).set(
            {"license_id": license_id, "client_name": client_name, "queue": []}
        )
        print(f"Added {license_id} queue in 'queues' collection. Update_time: {result.update_time}.")

    def _remove_license_queue(self, license_id: str) -> None:
        timestamp = self.db.collection(self.collection).document(license_id).delete()
        print(f"Removed {license_id} queue in 'queues' collection. Update_time: {timestamp}.")

    def _get_queues(self) -> list:
        snapshot_list = self.db.collection(self.collection).get()
        result = []
        for snapshot in snapshot_list:
            result.append(self.db.collection(self.collection).document(snapshot.id).get().to_dict())
        return result

    def _get_queue_by_license_id(self, license_id: str) -> dict:
        result: DocumentSnapshot = self.db.collection(self.collection).document(license_id).get()
        return result.to_dict()

    def _get_queue_by_client_name(self, client_name: str) -> list:
        snapshot_list = self.db.collection(self.collection).where('client_name', '==', client_name).get()
        result = []
        for snapshot in snapshot_list:
            result.append(snapshot.to_dict())
        return result

    def _update_license_queue(self, license_id: str, priority_list: list) -> None:
        result: WriteResult = self.db.collection(self.collection).document(license_id).update(
                {'queue': priority_list}
            )
        print(f"Updated {license_id} queue in 'queues' collection. Update_time: {result.update_time}.")

    def _update_queues_on_start_download(self, config_name: str, licenses: list) -> None:
        for license in licenses:
            result: WriteResult = self.db.collection(self.collection).document(license).update(
                {'queue': firestore.ArrayUnion([config_name])}
            )
            print(f"Updated {license} queue in 'queues' collection. Update_time: {result.update_time}.")

    def _update_queues_on_stop_download(self, config_name: str) -> None:
        snapshot_list = self.db.collection(self.collection).get()
        for snapshot in snapshot_list:
            result: WriteResult = self.db.collection(self.collection).document(snapshot.id).update({
                'queue': firestore.ArrayRemove([config_name])})
            print(f"Updated {snapshot.id} queue in 'queues' collection. Update_time: {result.update_time}.")