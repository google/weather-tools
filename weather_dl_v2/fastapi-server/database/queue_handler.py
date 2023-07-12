import abc
import logging
from firebase_admin import firestore
from google.cloud.firestore_v1 import DocumentSnapshot
from google.cloud.firestore_v1.types import WriteResult
from database.session import get_db

logger = logging.getLogger(__name__)

def get_queue_handler():
    return QueueHandlerFirestore(db=get_db())

def get_mock_queue_handler():
    return QueueHandlerMock()

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

    @abc.abstractmethod
    def _update_config_priority_in_license(self, license_id: str, config_name: str, priority: int) -> None:
        pass

class QueueHandlerMock(QueueHandler):
    def __init__(self):
        pass

    def _create_license_queue(self, license_id: str, client_name: str) -> None:
        logger.info(f"Added {license_id} queue in 'queues' collection. Update_time: 000000.")

    def _remove_license_queue(self, license_id: str) -> None:
        logger.info(f"Removed {license_id} queue in 'queues' collection. Update_time: 000000.")

    def _get_queues(self) -> list:
        return [
            {
                "client_name": "dummy_client",
                "license_id": "L1",
                "queue": []
            }
        ]

    def _get_queue_by_license_id(self, license_id: str) -> dict:
        if license_id == "no_exists":
            return None
        return {
                "client_name": "dummy_client",
                "license_id": license_id,
                "queue": []
            }

    def _get_queue_by_client_name(self, client_name: str) -> list:
        return [
            {
                "client_name": client_name,
                "license_id": "L1",
                "queue": []
            }
        ]

    def _update_license_queue(self, license_id: str, priority_list: list) -> None:
        logger.info(f"Updated {license_id} queue in 'queues' collection. Update_time: 00000.")

    def _update_queues_on_start_download(self, config_name: str, licenses: list) -> None:
        logger.info(f"Updated {license} queue in 'queues' collection. Update_time: 00000.")

    def _update_queues_on_stop_download(self, config_name: str) -> None:
        logger.info("Updated snapshot.id queue in 'queues' collection. Update_time: 00000.")

    def _update_config_priority_in_license(self, license_id: str, config_name: str, priority: int) -> None:
        print(f"Updated snapshot.id queue in 'queues' collection. Update_time: 00000.")

class QueueHandlerFirestore(QueueHandler):
    def __init__(self, db: firestore.firestore.Client, collection: str = "queues"):
        self.db = db
        self.collection = collection

    def _create_license_queue(self, license_id: str, client_name: str) -> None:
        result: WriteResult = self.db.collection(self.collection).document(license_id).set(
            {"license_id": license_id, "client_name": client_name, "queue": []}
        )
        logger.info(f"Added {license_id} queue in 'queues' collection. Update_time: {result.update_time}.")

    def _remove_license_queue(self, license_id: str) -> None:
        timestamp = self.db.collection(self.collection).document(license_id).delete()
        logger.info(f"Removed {license_id} queue in 'queues' collection. Update_time: {timestamp}.")

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
        logger.info(f"Updated {license_id} queue in 'queues' collection. Update_time: {result.update_time}.")

    def _update_queues_on_start_download(self, config_name: str, licenses: list) -> None:
        for license in licenses:
            result: WriteResult = self.db.collection(self.collection).document(license).update(
                {'queue': firestore.ArrayUnion([config_name])}
            )
            logger.info(f"Updated {license} queue in 'queues' collection. Update_time: {result.update_time}.")

    def _update_queues_on_stop_download(self, config_name: str) -> None:
        snapshot_list = self.db.collection(self.collection).get()
        for snapshot in snapshot_list:
            result: WriteResult = self.db.collection(self.collection).document(snapshot.id).update({
                'queue': firestore.ArrayRemove([config_name])})
            logger.info(f"Updated {snapshot.id} queue in 'queues' collection. Update_time: {result.update_time}.")

    def _update_config_priority_in_license(self, license_id: str, config_name: str, priority: int) -> None:
        snapshot: DocumentSnapshot = self._get_db().collection('queues').document(license_id).get()
        priority_list = snapshot.to_dict()['queue']
        if config_name not in priority_list:
            print(f"'{config_name}' not in queue.")
            raise
        new_priority_list = [c for c in priority_list if c != config_name]
        new_priority_list.insert(priority, config_name)
        result: WriteResult = self._get_db().collection('queues').document(license_id).update(
            {'queue': new_priority_list}
        )
        print(f"Updated {snapshot.id} queue in 'queues' collection. Update_time: {result.update_time}.")
