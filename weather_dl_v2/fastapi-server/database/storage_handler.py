import abc
import os
import logging
import tempfile
import contextlib
import typing as t
from google.cloud import storage
from database.session import get_gcs_client
from server_config import get_config


logger = logging.getLogger(__name__)

def get_storage_handler():
    return StorageHandlerGCS(client=get_gcs_client())


class StorageHandler(abc.ABC):

    @abc.abstractmethod
    def _upload_file(self, bucket_name, file_path) -> str:
        pass

    @abc.abstractmethod
    def _open_local(self, bucket_name, file_name) -> t.Iterator[str]:
        pass

class StorageHandlerMock(StorageHandler):

    def __init__(self) -> None:
        pass

    def _upload_file(self, bucket_name, file_path) -> None:
        pass

    def _open_local(self, bucket_name, file_name) -> t.Iterator[str]:
        pass


class StorageHandlerGCS(StorageHandler):

    def __init__(self, client: storage.Client) -> None:
        self.client = client

    def _upload_file(self, bucket_name, file_path) -> str:
        filename = os.path.basename(file_path).split('/')[-1]
        bucket = self.client.get_bucket(bucket_name)
        blob = bucket.blob(filename)
        blob.upload_from_filename(file_path)

        logger.info(f"Uploaded {filename} to {bucket}.")
        return blob.public_url
    
    @contextlib.contextmanager
    def _open_local(self, bucket_name, file_name) -> t.Iterator[str]:
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        with tempfile.NamedTemporaryFile() as dest_file:
            blob.download_to_filename(dest_file.name)
            yield dest_file.name
