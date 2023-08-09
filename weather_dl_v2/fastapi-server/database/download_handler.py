import abc
import logging
from firebase_admin import firestore
from google.cloud.firestore_v1 import DocumentSnapshot, FieldFilter
from google.cloud.firestore_v1.types import WriteResult
from database.session import get_async_client
from server_config import get_config

logger = logging.getLogger(__name__)


def get_download_handler():
    return DownloadHandlerFirestore(db=get_async_client())


def get_mock_download_handler():
    return DownloadHandlerMock()


class DownloadHandler(abc.ABC):

    @abc.abstractmethod
    async def _start_download(self, config_name: str, client_name: str) -> None:
        pass

    @abc.abstractmethod
    async def _stop_download(self, config_name: str) -> None:
        pass

    @abc.abstractmethod
    async def _mark_partitioning_status(self, config_name: str, status: str) -> None:
        pass

    @abc.abstractmethod
    async def _check_download_exists(self, config_name: str) -> bool:
        pass

    @abc.abstractmethod
    async def _get_downloads(self, client_name: str) -> list:
        pass

    @abc.abstractmethod
    async def _get_download_by_config_name(self, config_name: str):
        pass


class DownloadHandlerMock(DownloadHandler):

    def __init__(self):
        pass

    async def _start_download(self, config_name: str, client_name: str) -> None:
        logger.info(
            f"Added {config_name} in 'download' collection. Update_time: 000000."
        )

    async def _stop_download(self, config_name: str) -> None:
        logger.info(
            f"Removed {config_name} in 'download' collection. Update_time: 000000."
        )

    async def _mark_partitioning_status(self, config_name: str, status: str) -> None:
        logger.info(
            f"Updated {config_name} in 'download' collection. Update_time: 000000."
        )

    async def _check_download_exists(self, config_name: str) -> bool:
        if config_name == "no_exist":
            return False
        elif config_name == "no_exist.cfg":
            return False
        else:
            return True

    async def _get_downloads(self, client_name: str) -> list:
        return [{"config_name": "example.cfg", "client_name": "client"}]

    async def _get_download_by_config_name(self, config_name: str):
        return {"config_name": "example.cfg", "client_name": "client"}


class DownloadHandlerFirestore(DownloadHandler):

    def __init__(self, db: firestore.firestore.Client):
        self.db = db
        self.collection = get_config().download_collection

    async def _start_download(self, config_name: str, client_name: str) -> None:
        result: WriteResult = (
            await self.db.collection(self.collection)
            .document(config_name)
            .set({"config_name": config_name, "client_name": client_name})
        )

        logger.info(
            f"Added {config_name} in 'download' collection. Update_time: {result.update_time}."
        )

    async def _stop_download(self, config_name: str) -> None:
        timestamp = (
            await self.db.collection(self.collection).document(config_name).delete()
        )
        logger.info(
            f"Removed {config_name} in 'download' collection. Update_time: {timestamp}."
        )

    async def _mark_partitioning_status(self, config_name: str, status: str) -> None:
        timestamp = (
            await self.db.collection(self.collection)
            .document(config_name)
            .update({"status": status})
        )
        logger.info(
            f"Updated {config_name} in 'download' collection. Update_time: {timestamp}."
        )

    async def _check_download_exists(self, config_name: str) -> bool:
        result: DocumentSnapshot = (
            await self.db.collection(self.collection).document(config_name).get()
        )
        return result.exists

    async def _get_downloads(self, client_name: str) -> list:
        docs = []
        if client_name:
            docs = (
                self.db.collection(self.collection)
                .where(filter=FieldFilter("client_name", "==", client_name))
                .stream()
            )
        else:
            docs = self.db.collection(self.collection).stream()

        return [doc.to_dict() async for doc in docs]

    async def _get_download_by_config_name(self, config_name: str):
        result: DocumentSnapshot = (
            await self.db.collection(self.collection).document(config_name).get()
        )
        if result.exists:
            return result.to_dict()
        else:
            return None
