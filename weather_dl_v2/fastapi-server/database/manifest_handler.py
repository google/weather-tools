import abc
import logging
from firebase_admin import firestore
from google.cloud.firestore_v1.base_query import FieldFilter, Or, And
from server_config import get_config
from database.session import get_async_client

logger = logging.getLogger(__name__)


def get_manifest_handler():
    return ManifestHandlerFirestore(db=get_async_client())


def get_mock_manifest_handler():
    return ManifestHandlerMock()


class ManifestHandler(abc.ABC):

    @abc.abstractmethod
    async def _get_download_success_count(self, config_name: str) -> int:
        pass

    @abc.abstractmethod
    async def _get_download_failure_count(self, config_name: str) -> int:
        pass

    @abc.abstractmethod
    async def _get_download_scheduled_count(self, config_name: str) -> int:
        pass

    @abc.abstractmethod
    async def _get_download_inprogress_count(self, config_name: str) -> int:
        pass

    @abc.abstractmethod
    async def _get_download_total_count(self, config_name: str) -> int:
        pass


class ManifestHandlerMock(ManifestHandler):

    async def _get_download_failure_count(self, config_name: str) -> int:
        return 0

    async def _get_download_inprogress_count(self, config_name: str) -> int:
        return 0

    async def _get_download_scheduled_count(self, config_name: str) -> int:
        return 0

    async def _get_download_success_count(self, config_name: str) -> int:
        return 0

    async def _get_download_total_count(self, config_name: str) -> int:
        return 0


class ManifestHandlerFirestore(ManifestHandler):

    def __init__(self, db: firestore.firestore.Client):
        self.db = db
        self.collection = get_config().manifest_collection

    async def _get_download_success_count(self, config_name: str) -> int:
        result = (
            await self.db.collection(self.collection)
            .where("config_name", "==", config_name)
            .where("stage", "==", "upload")
            .where("status", "==", "success")
            .count()
            .get()
        )

        count = result[0][0].value

        return count

    async def _get_download_failure_count(self, config_name: str) -> int:
        result = (
            await self.db.collection(self.collection)
            .where("config_name", "==", config_name)
            .where("status", "==", "failure")
            .count()
            .get()
        )

        count = result[0][0].value

        return count

    async def _get_download_scheduled_count(self, config_name: str) -> int:
        result = (
            await self.db.collection(self.collection)
            .where("config_name", "==", config_name)
            .where("status", "==", "scheduled")
            .count()
            .get()
        )

        count = result[0][0].value

        return count

    async def _get_download_inprogress_count(self, config_name: str) -> int:
        and_filter = And(
            filters=[
                FieldFilter("status", "==", "success"),
                FieldFilter("stage", "!=", "upload"),
            ]
        )
        or_filter = Or(filters=[FieldFilter("status", "==", "in-progress"), and_filter])

        result = (
            await self.db.collection(self.collection)
            .where("config_name", "==", config_name)
            .where(filter=or_filter)
            .count()
            .get()
        )

        count = result[0][0].value

        return count

    async def _get_download_total_count(self, config_name: str) -> int:
        result = (
            await self.db.collection(self.collection)
            .where("config_name", "==", config_name)
            .count()
            .get()
        )

        count = result[0][0].value

        return count
