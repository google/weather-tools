import time
import abc
import logging
import firebase_admin
from firebase_admin import firestore
from firebase_admin import credentials
from config_processing.util import get_wait_interval

logger = logging.getLogger(__name__)

class Database(abc.ABC):
    @abc.abstractmethod
    def _get_db(self):
        pass

def get_db() -> firestore.firestore.Client:
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
            logger.info('Initialized Firebase App.')

            if attempts > 4:
                raise RuntimeError('Exceeded number of retries to get firestore client.') from e

        time.sleep(get_wait_interval(attempts))

        attempts += 1

    return db
