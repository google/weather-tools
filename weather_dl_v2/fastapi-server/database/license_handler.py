import abc
import logging
from firebase_admin import firestore
from google.cloud.firestore_v1 import DocumentSnapshot, FieldFilter
from google.cloud.firestore_v1.types import WriteResult
from database.session import get_async_client
from server_config import get_config


logger = logging.getLogger(__name__)


def get_license_handler():
    return LicenseHandlerFirestore(db=get_async_client())


def get_mock_license_handler():
    return LicenseHandlerMock()


class LicenseHandler(abc.ABC):

    @abc.abstractmethod
    async def _add_license(self, license_dict: dict) -> str:
        pass

    @abc.abstractmethod
    async def _delete_license(self, license_id: str) -> None:
        pass

    @abc.abstractmethod
    async def _check_license_exists(self, license_id: str) -> bool:
        pass

    @abc.abstractmethod
    async def _get_license_by_license_id(self, license_id: str) -> dict:
        pass

    @abc.abstractmethod
    async def _get_license_by_client_name(self, client_name: str) -> list:
        pass

    @abc.abstractmethod
    async def _get_licenses(self) -> list:
        pass

    @abc.abstractmethod
    async def _update_license(self, license_id: str, license_dict: dict) -> None:
        pass

    @abc.abstractmethod
    async def _get_license_without_deployment(self) -> list:
        pass


class LicenseHandlerMock(LicenseHandler):

    def __init__(self):
        pass

    async def _add_license(self, license_dict: dict) -> str:
        license_id = "L1"
        logger.info(f"Added {license_id} in 'license' collection. Update_time: 00000.")
        return license_id

    async def _delete_license(self, license_id: str) -> None:
        logger.info(
            f"Removed {license_id} in 'license' collection. Update_time: 00000."
        )

    async def _update_license(self, license_id: str, license_dict: dict) -> None:
        logger.info(
            f"Updated {license_id} in 'license' collection. Update_time: 00000."
        )

    async def _check_license_exists(self, license_id: str) -> bool:
        if license_id == "no_exists":
            return False
        else:
            return True

    async def _get_license_by_license_id(self, license_id: str) -> dict:
        if license_id == "no_exists":
            return None
        return {
            "license_id": license_id,
            "secret_id": "xxxx",
            "client_name": "dummy_client",
            "k8s_deployment_id": "k1",
            "number_of_requets": 100,
        }

    async def _get_license_by_client_name(self, client_name: str) -> list:
        return [{
            "license_id": "L1",
            "secret_id": "xxxx",
            "client_name": client_name,
            "k8s_deployment_id": "k1",
            "number_of_requets": 100,
        }]

    async def _get_licenses(self) -> list:
        return [{
            "license_id": "L1",
            "secret_id": "xxxx",
            "client_name": "dummy_client",
            "k8s_deployment_id": "k1",
            "number_of_requets": 100,
        }]

    async def _get_license_without_deployment(self) -> list:
        return []


class LicenseHandlerFirestore(LicenseHandler):

    def __init__(self, db: firestore.firestore.AsyncClient):
        self.db = db
        self.collection = get_config().license_collection

    async def _add_license(self, license_dict: dict) -> str:
        license_id = license_dict['license_id'].lower()
        license_dict['license_id'] = license_dict['license_id'].lower()
        result: WriteResult = (
            await self.db.collection(self.collection)
            .document(license_id)
            .set(license_dict)
        )
        logger.info(
            f"Added {license_id} in 'license' collection. Update_time: {result.update_time}."
        )
        return license_id

    async def _delete_license(self, license_id: str) -> None:
        timestamp = (
            await self.db.collection(self.collection).document(license_id).delete()
        )
        logger.info(
            f"Removed {license_id} in 'license' collection. Update_time: {timestamp}."
        )

    async def _update_license(self, license_id: str, license_dict: dict) -> None:
        result: WriteResult = (
            await self.db.collection(self.collection)
            .document(license_id)
            .update(license_dict)
        )
        logger.info(
            f"Updated {license_id} in 'license' collection. Update_time: {result.update_time}."
        )

    async def _check_license_exists(self, license_id: str) -> bool:
        result: DocumentSnapshot = (
            await self.db.collection(self.collection).document(license_id).get()
        )
        return result.exists

    async def _get_license_by_license_id(self, license_id: str) -> dict:
        result: DocumentSnapshot = (
            await self.db.collection(self.collection).document(license_id).get()
        )
        return result.to_dict()

    async def _get_license_by_client_name(self, client_name: str) -> list:
        docs = (
            self.db.collection(self.collection)
            .where(filter=FieldFilter("client_name", "==", client_name))
            .stream()
        )
        return [doc.to_dict() async for doc in docs]

    async def _get_licenses(self) -> list:
        docs = self.db.collection(self.collection).stream()
        return [doc.to_dict() async for doc in docs]

    async def _get_license_without_deployment(self) -> list:
        docs = (
            self.db.collection(self.collection)
            .where(filter=FieldFilter("k8s_deployment_id", "==", ""))
            .stream()
        )
        return [doc.to_dict() async for doc in docs]
