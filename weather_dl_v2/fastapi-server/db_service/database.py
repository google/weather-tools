import abc
import time
import firebase_admin
from firebase_admin import firestore
from firebase_admin import credentials
from google.cloud.firestore_v1 import DocumentSnapshot
from google.cloud.firestore_v1.types import WriteResult
from config_processing.util import get_wait_interval


class Database(abc.ABC):
    @abc.abstractmethod
    def _get_db(self):
        pass


class CRUDOperations(abc.ABC):
    @abc.abstractmethod
    def _start_download(self, config_name: str, client_name: str) -> None:
        pass

    @abc.abstractmethod
    def _stop_download(self, config_name: str) -> None:
        pass

    @abc.abstractmethod
    def _check_download_exists(self, config_name: str) -> bool:
        pass

    @abc.abstractmethod
    def _add_license(self, license_dict: dict) -> str:
        pass

    @abc.abstractmethod
    def _delete_license(self, license_id: str) -> str:
        pass

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
    def _update_config_priority__in_license(self, license_id: str, config_name: str, priority: int) -> None:
        pass

    @abc.abstractmethod
    def _check_license_exists(self, license_id: str) -> bool:
        pass

    @abc.abstractmethod
    def _get_license_by_license_id(slef, license_id: str) -> dict:
        pass

    @abc.abstractmethod
    def _get_license_by_client_name(self, client_name: str) -> list:
        pass

    @abc.abstractmethod
    def _get_licenses(self) -> list:
        pass

    @abc.abstractmethod
    def _update_license(self, license_id: str, license_dict: dict) -> None:
        pass

    # TODO: Find better way to execute these query.
    # @abc.abstractmethod
    # def _get_download_by_config_name(self, config_name: str) -> dict:
    #     pass

    # @abc.abstractmethod
    # def _get_dowloads(self) -> list:
    #     pass

    @abc.abstractmethod
    def _update_queues_on_start_download(self, config_name: str, licenses: list) -> None:
        pass

    @abc.abstractmethod
    def _update_queues_on_stop_download(self, config_name: str) -> None:
        pass


class FirestoreClient(Database, CRUDOperations):
    def _get_db(self) -> firestore.firestore.Client:
        """Acquire a firestore client, initializing the firebase app if necessary.
        Will attempt to get the db client five times. If it's still unsuccessful, a
        `ManifestException` will be raised.
        """
        db = None
        attempts = 0

        while db is None:
            try:
                db = firestore.client()
            except ValueError as e:
                # The above call will fail with a value error when the firebase app is not initialized.
                # Initialize the app here, and try again.
                # Use the application default credentials.
                cred = credentials.ApplicationDefault()

                firebase_admin.initialize_app(cred)
                print('Initialized Firebase App.')

                if attempts > 4:
                    raise RuntimeError('Exceeded number of retries to get firestore client.') from e

            time.sleep(get_wait_interval(attempts))

            attempts += 1

        return db

    def _start_download(self, config_name: str, client_name: str) -> None:
        result: WriteResult = self._get_db().collection('download').document(config_name).set(
            {'config_name': config_name, 'client_name': client_name}
            )

        print(f"Added {config_name} in 'download' collection. Update_time: {result.update_time}.")

    def _stop_download(self, config_name: str) -> None:
        timestamp = self._get_db().collection('download').document(config_name).delete()
        print(f"Removed {config_name} in 'download' collection. Update_time: {timestamp}.")

    def _check_download_exists(self, config_name: str) -> bool:
        result: DocumentSnapshot = self._get_db().collection('download').document(config_name).get()
        return result.exists

    def _add_license(self, license_dict: dict) -> str:
        license_id = f"L{len(self._get_db().collection('license').get()) + 1}"
        license_dict["license_id"] = license_id
        result: WriteResult = self._get_db().collection('license').document(license_id).set(
            license_dict
        )
        print(f"Added {license_id} in 'license' collection. Update_time: {result.update_time}.")
        return license_id

    def _delete_license(self, license_id: str) -> None:
        timestamp = self._get_db().collection('license').document(license_id).delete()
        print(f"Removed {license_id} in 'license' collection. Update_time: {timestamp}.")

    def _update_license(self, license_id: str, license_dict: dict) -> None:
        result: WriteResult = self._get_db().collection('license').document(license_id).update(license_dict)
        print(f"Updated {license_id} in 'license' collection. Update_time: {result.update_time}.")

    def _create_license_queue(self, license_id: str, client_name: str) -> None:
        result: WriteResult = self._get_db().collection('queues').document(license_id).set(
            {"license_id": license_id, "client_name": client_name, "queue": []}
        )
        print(f"Added {license_id} queue in 'queues' collection. Update_time: {result.update_time}.")

    def _remove_license_queue(self, license_id: str) -> None:
        timestamp = self._get_db().collection('queues').document(license_id).delete()
        print(f"Removed {license_id} queue in 'queues' collection. Update_time: {timestamp}.")

    def _get_queues(self) -> list:
        snapshot_list = self._get_db().collection('queues').get()
        result = []
        for snapshot in snapshot_list:
            result.append(self._get_db().collection('queues').document(snapshot.id).get().to_dict())
        return result

    def _get_queue_by_license_id(self, license_id: str) -> dict:
        result: DocumentSnapshot = self._get_db().collection('queues').document(license_id).get()
        return result.to_dict()

    def _get_queue_by_client_name(self, client_name: str) -> list:
        snapshot_list = self._get_db().collection('queues').where('client_name', '==', client_name).get()
        result = []
        for snapshot in snapshot_list:
            result.append(snapshot.to_dict())
        return result

    def _update_license_queue(self, license_id: str, priority_list: list) -> None:
        result: WriteResult = self._get_db().collection('queues').document(license).update(
                {'queue': priority_list}
            )
        print(f"Updated {license_id} queue in 'queues' collection. Update_time: {result.update_time}.")

    def _update_config_priority__in_license(self, license_id: str, config_name: str, priority: int) -> None:
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

    def _check_license_exists(self, license_id: str) -> bool:
        result: DocumentSnapshot = self._get_db().collection('license').document(license_id).get()
        return result.exists

    def _get_license_by_license_id(self, license_id: str) -> dict:
        result: DocumentSnapshot = self._get_db().collection('license').document(license_id).get()
        return result.to_dict()

    def _get_license_by_client_name(self, client_name: str) -> list:
        snapshot_list = self._get_db().collection('license').where('client_name', '==', client_name).get()
        result = []
        for snapshot in snapshot_list:
            result.append(snapshot.to_dict())
        return result

    def _get_licenses(self) -> list:
        snapshot_list = self._get_db().collection('license').get()
        result = []
        for snapshot in snapshot_list:
            result.append(self._get_db().collection('license').document(snapshot.id).get().to_dict())
        return result

    # def _get_download_by_config_name(self, config_name: str) -> dict:
    #     result: DocumentSnapshot = self._get_db().collection('download_status').document(config_name).get()
    #     return result.to_dict()

    # def _get_dowloads(self) -> list:
    #     pass

    def _update_queues_on_start_download(self, config_name: str, licenses: list) -> None:
        for license in licenses:
            result: WriteResult = self._get_db().collection('queues').document(license).update(
                {'queue': firestore.ArrayUnion([config_name])}
            )
            print(f"Updated {license} queue in 'queues' collection. Update_time: {result.update_time}.")

    def _update_queues_on_stop_download(self, config_name: str) -> None:
        snapshot_list = self._get_db().collection('queues').get()
        for snapshot in snapshot_list:
            result: WriteResult = self._get_db().collection('queues').document(snapshot.id).update({
                'queue': firestore.ArrayRemove([config_name])})
            print(f"Updated {snapshot.id} queue in 'queues' collection. Update_time: {result.update_time}.")
