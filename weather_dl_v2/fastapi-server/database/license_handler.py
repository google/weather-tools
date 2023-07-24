import abc
import logging
from firebase_admin import firestore
from google.cloud.firestore_v1 import DocumentSnapshot
from google.cloud.firestore_v1.types import WriteResult
from database.session import get_db

logger = logging.getLogger(__name__)


def get_license_handler():
    return LicenseHandlerFirestore(db=get_db())


def get_mock_license_handler():
    return LicenseHandlerMock()


class LicenseHandler(abc.ABC):

    @abc.abstractmethod
    def _add_license(self, license_dict: dict) -> str:
        pass

    @abc.abstractmethod
    def _delete_license(self, license_id: str) -> None:
        pass

    @abc.abstractmethod
    def _check_license_exists(self, license_id: str) -> bool:
        pass

    @abc.abstractmethod
    def _get_license_by_license_id(self, license_id: str) -> dict:
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

    @abc.abstractmethod
    def _get_license_without_deployment(self) -> list:
        pass


class LicenseHandlerMock(LicenseHandler):

    def __init__(self):
        pass

    def _add_license(self, license_dict: dict) -> str:
        license_id = "L1"
        logger.info(f"Added {license_id} in 'license' collection. Update_time: 00000.")
        return license_id

    def _delete_license(self, license_id: str) -> None:
        logger.info(
            f"Removed {license_id} in 'license' collection. Update_time: 00000."
        )

    def _update_license(self, license_id: str, license_dict: dict) -> None:
        logger.info(
            f"Updated {license_id} in 'license' collection. Update_time: 00000."
        )

    def _check_license_exists(self, license_id: str) -> bool:
        if license_id == "no_exists":
            return False
        else:
            return True

    def _get_license_by_license_id(self, license_id: str) -> dict:
        if license_id == "no_exists":
            return None
        return {
            "license_id": license_id,
            "secret_id": "xxxx",
            "client_name": "dummy_client",
            "k8s_deployment_id": "k1",
            "number_of_requets": 100,
        }

    def _get_license_by_client_name(self, client_name: str) -> list:
        return [{
            "license_id": "L1",
            "secret_id": "xxxx",
            "client_name": client_name,
            "k8s_deployment_id": "k1",
            "number_of_requets": 100,
        }]

    def _get_licenses(self) -> list:
        return [{
            "license_id": "L1",
            "secret_id": "xxxx",
            "client_name": "dummy_client",
            "k8s_deployment_id": "k1",
            "number_of_requets": 100,
        }]

    def _get_license_without_deployment(self) -> list:
        return []


class LicenseHandlerFirestore(LicenseHandler):

    def __init__(self, db: firestore.firestore.Client):
        self.db = db
        self.collection = "license"

    # TODO: find alternative way to create license_id
    def _add_license(self, license_dict: dict) -> str:
        license_id = f"L{len(self.db.collection(self.collection).get()) + 1}"
        license_dict["license_id"] = license_id
        result: WriteResult = (
            self.db.collection(self.collection).document(license_id).set(license_dict)
        )
        logger.info(
            f"Added {license_id} in 'license' collection. Update_time: {result.update_time}."
        )
        return license_id

    def _delete_license(self, license_id: str) -> None:
        timestamp = self.db.collection(self.collection).document(license_id).delete()
        logger.info(
            f"Removed {license_id} in 'license' collection. Update_time: {timestamp}."
        )

    def _update_license(self, license_id: str, license_dict: dict) -> None:
        result: WriteResult = (
            self.db.collection(self.collection)
            .document(license_id)
            .update(license_dict)
        )
        logger.info(
            f"Updated {license_id} in 'license' collection. Update_time: {result.update_time}."
        )

    def _check_license_exists(self, license_id: str) -> bool:
        result: DocumentSnapshot = (
            self.db.collection(self.collection).document(license_id).get()
        )
        return result.exists

    def _get_license_by_license_id(self, license_id: str) -> dict:
        result: DocumentSnapshot = (
            self.db.collection(self.collection).document(license_id).get()
        )
        return result.to_dict()

    def _get_license_by_client_name(self, client_name: str) -> list:
        snapshot_list = (
            self.db.collection(self.collection)
            .where("client_name", "==", client_name)
            .get()
        )
        result = []
        for snapshot in snapshot_list:
            result.append(snapshot.to_dict())
        return result

    def _get_licenses(self) -> list:
        snapshot_list = self.db.collection(self.collection).get()
        result = []
        for snapshot in snapshot_list:
            result.append(
                self.db.collection(self.collection)
                .document(snapshot.id)
                .get()
                .to_dict()
            )
        return result

    def _get_license_without_deployment(self) -> list:
        snapshot_list = (
            self.db.collection(self.collection)
            .where("k8s_deployment_id", "==", "")
            .get()
        )
        result = []
        for snapshot in snapshot_list:
            result.append(snapshot.to_dict()["license_id"])
        return result
