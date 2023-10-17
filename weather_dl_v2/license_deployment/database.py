# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import abc
import time
import logging
import firebase_admin
from firebase_admin import firestore
from firebase_admin import credentials
from google.cloud.firestore_v1 import DocumentSnapshot, DocumentReference
from google.cloud.firestore_v1.types import WriteResult
from google.cloud.firestore_v1.base_query import FieldFilter, And
from util import get_wait_interval
from deployment_config import get_config

logger = logging.getLogger(__name__)


class Database(abc.ABC):

    @abc.abstractmethod
    def _get_db(self):
        pass


class CRUDOperations(abc.ABC):

    @abc.abstractmethod
    def _initialize_license_deployment(self, license_id: str) -> dict:
        pass

    @abc.abstractmethod
    def _get_config_from_queue_by_license_id(self, license_id: str) -> dict:
        pass

    @abc.abstractmethod
    def _remove_config_from_license_queue(
        self, license_id: str, config_name: str
    ) -> None:
        pass

    @abc.abstractmethod
    def _get_partition_from_manifest(self, config_name: str) -> str:
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
                logger.info("Initialized Firebase App.")

                if attempts > 4:
                    raise RuntimeError(
                        "Exceeded number of retries to get firestore client."
                    ) from e

            time.sleep(get_wait_interval(attempts))

            attempts += 1

        return db

    def _initialize_license_deployment(self, license_id: str) -> dict:
        result: DocumentSnapshot = (
            self._get_db()
            .collection(get_config().license_collection)
            .document(license_id)
            .get()
        )
        return result.to_dict()

    def _get_config_from_queue_by_license_id(self, license_id: str) -> str | None:
        result: DocumentSnapshot = (
            self._get_db()
            .collection(get_config().queues_collection)
            .document(license_id)
            .get(["queue"])
        )
        if result.exists:
            queue = result.to_dict()["queue"]
            if len(queue) > 0:
                return queue[0]
        return None

    def _get_partition_from_manifest(self, config_name: str) -> str | None:
        transaction = self._get_db().transaction()
        return get_partition_from_manifest(transaction, config_name)

    def _remove_config_from_license_queue(
        self, license_id: str, config_name: str
    ) -> None:
        result: WriteResult = (
            self._get_db()
            .collection(get_config().queues_collection)
            .document(license_id)
            .update({"queue": firestore.ArrayRemove([config_name])})
        )
        logger.info(
            f"Updated {license_id} queue in 'queues' collection. Update_time: {result.update_time}."
        )


@firestore.transactional
def get_partition_from_manifest(transaction, config_name: str) -> str | None:
    db_client = FirestoreClient()
    filter_1 = FieldFilter("config_name", "==", config_name)
    filter_2 = FieldFilter("status", "==", "scheduled")
    and_filter = And(filters=[filter_1, filter_2])

    snapshot = (
        db_client._get_db()
        .collection(get_config().manifest_collection)
        .where(filter=and_filter)
        .limit(1)
        .get(transaction=transaction)
    )
    if len(snapshot) > 0:
        snapshot = snapshot[0]
    else:
        return None

    ref: DocumentReference = (
        db_client._get_db()
        .collection(get_config().manifest_collection)
        .document(snapshot.id)
    )
    transaction.update(ref, {"status": "processing"})

    return snapshot.to_dict()
