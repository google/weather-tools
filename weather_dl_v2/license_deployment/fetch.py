from concurrent.futures import ThreadPoolExecutor
from google.cloud import secretmanager
import json
import logging
import time
import sys
import os

from database import FirestoreClient
from job_creator import create_download_job
from clients import CLIENTS
from manifest import FirestoreManifest
from util import exceptionit

db_client = FirestoreClient()
secretmanager_client = secretmanager.SecretManagerServiceClient()

def create_job(request, result):
    res = {
        "config_name": request["config_name"],
        "dataset": request["dataset"],
        "selection": json.loads(request["selection"]),
        "user_id": request["username"],
        "url": result["href"],
        "target_path": request["location"],
        "license_id": license_id,
    }

    data_str = json.dumps(res)
    logger.info(f"Creating download job for res: {data_str}")
    create_download_job(data_str)


@exceptionit
def make_fetch_request(request):
    client = CLIENTS[client_name](request["dataset"])
    manifest = FirestoreManifest(license_id=license_id)
    logger.info(
        f"By using {client_name} datasets, "
        f"users agree to the terms and conditions specified in {client.license_url!r}"
    )

    target = request["location"]
    selection = json.loads(request["selection"])

    logger.info(f"Fetching data for {target!r}.")
    with manifest.transact(
        request["config_name"],
        request["dataset"],
        selection,
        target,
        request["username"],
    ):
        result = client.retrieve(request["dataset"], selection, manifest)

    create_job(request, result)


def fetch_request_from_db():
    request = None
    config_name = db_client._get_config_from_queue_by_license_id(license_id)
    if config_name:
        try:
            logger.info(f"Fetching partition for {config_name}")
            request = db_client._get_partition_from_manifest(config_name)
            if not request:
                db_client._remove_config_from_license_queue(license_id, config_name)
        except Exception as e:
            logger.error(f"Error in fetch_request_from_db for {config_name}. error: {e}.")
    return request


def main():
    logger.info("Started looking at the request.")
    with ThreadPoolExecutor(concurrency_limit) as executor:
        while True:
            # Fetch a request from the database
            request = fetch_request_from_db()

            if request is not None:
                executor.submit(make_fetch_request, request)
            else:
                logger.info("No request available. Waiting...")
                time.sleep(5)

            # Check if the maximum concurrency level has been reached
            # If so, wait for a slot to become available
            while executor._work_queue.qsize() >= concurrency_limit:
                time.sleep(1)


def boot_up(license: str) -> None:
    global license_id, client_name, concurrency_limit

    result = db_client._initialize_license_deployment(license)
    license_id = license
    client_name = result["client_name"]
    concurrency_limit = result["number_of_requests"]

    response = secretmanager_client.access_secret_version(
        request={"name": result["secret_id"]}
    )
    payload = response.payload.data.decode("UTF-8")
    secret_dict = json.loads(payload)

    os.environ.setdefault("CLIENT_URL", secret_dict.get("api_url", ""))
    os.environ.setdefault("CLIENT_KEY", secret_dict.get("api_key", ""))
    os.environ.setdefault("CLIENT_EMAIL", secret_dict.get("api_email", ""))


if __name__ == "__main__":
    license = sys.argv[2]
    global logger
    logging.basicConfig(level=logging.INFO, format=f'[{license}] %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    logger.info(f"Deployment for license: {license}.")
    boot_up(license)
    main()
