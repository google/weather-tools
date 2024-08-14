# Copyright 2023 Google LLC
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


from concurrent.futures import ThreadPoolExecutor
from google.cloud import secretmanager
import json
import logging
import time
import sys
import os
import google.cloud.logging

from database import FirestoreClient
from job_creator import create_download_job
from clients import CLIENTS
from manifest import FirestoreManifest
from util import exceptionit, ThreadSafeDict, GracefulKiller

db_client = FirestoreClient()
log_client = google.cloud.logging.Client()
log_client.setup_logging()
secretmanager_client = secretmanager.SecretManagerServiceClient()
CONFIG_MAX_ERROR_COUNT = 10

logger = logging.getLogger(__name__)

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
def make_fetch_request(request, error_map: ThreadSafeDict):
    client = CLIENTS[client_name](request["dataset"])
    manifest = FirestoreManifest(license_id=license_id)
    logger.info(
        f"By using {client_name} datasets, "
        f"users agree to the terms and conditions specified in {client.license_url!r}."
    )

    target = request["location"]
    selection = json.loads(request["selection"])

    logger.info(f"Fetching data for {target!r}.")

    config_name = request["config_name"]

    if not error_map.has_key(config_name):
        error_map[config_name] = 0

    if error_map[config_name] >= CONFIG_MAX_ERROR_COUNT:
        logger.info(f"Error count for config {config_name} exceeded CONFIG_MAX_ERROR_COUNT ({CONFIG_MAX_ERROR_COUNT}).")
        error_map.remove(config_name)
        logger.info(f"Removing config {config_name} from license queue.")
        # Remove config from this license queue.
        db_client._remove_config_from_license_queue(license_id=license_id, config_name=config_name)
        return

    # Wait for exponential time based on error count.
    if error_map[config_name] > 0:
        logger.info(f"Error count for  config {config_name}: {error_map[config_name]}.")
        sleep_time = error_map.exponential_time(config_name)
        logger.info(f"Sleeping for {sleep_time} secs.")
        time.sleep(sleep_time)

    try:
        with manifest.transact(
            request["config_name"],
            request["dataset"],
            selection,
            target,
            request["username"],
        ):
            result = client.retrieve(request["dataset"], selection, manifest)
    except Exception as e:
        # We are handling this as generic case as CDS client throws generic exceptions.

        # License expired.
        if "Access token expired" in str(e):
            logger.error(f"{license_id} expired. Emptying queue! error: {e}.")
            db_client._empty_license_queue(license_id=license_id)
            db_client._mark_license_status(license_id, "License Expired.")
            return

        if "Access token disabled'" in str(e):
            logger.error(f"{license_id} disabled. Emptying queue! error: {e}.")
            db_client._empty_license_queue(license_id=license_id)
            db_client._mark_license_status(license_id, "License Disabled.")
            return

        # License queue full on client side.
        if "USER_QUEUED_LIMIT_EXCEEDED" in str(e) or \
            "Too many queued requests" in str(e):
            logger.error(f"{license_id} queue full. Emptying queue! error: {e}.")
            db_client._empty_license_queue(license_id=license_id)
            db_client._mark_license_status(license_id, "License Queue Full.")
            return

        # Increment error count for a config.
        logger.error(f"Partition fetching failed. Error {e}.")
        error_map.increment(config_name)
        return

    # If any partition in successful reset the error count.
    error_map[config_name] = 0
    create_job(request, result)


def fetch_request_from_db():
    request = None
    config_name = db_client._get_config_from_queue_by_license_id(license_id)
    if config_name:
        try:
            logger.info(f"Fetching partition for {config_name}.")
            request = db_client._get_partition_from_manifest(config_name)
            if not request:
                db_client._remove_config_from_license_queue(license_id, config_name)
        except Exception as e:
            logger.error(
                f"Error in fetch_request_from_db for {config_name}. error: {e}."
            )
    return request


def main():
    logger.info("Started looking at the request.")
    error_map = ThreadSafeDict()
    killer = GracefulKiller()
    with ThreadPoolExecutor(concurrency_limit) as executor:
        # Disclaimer: A license will pick always pick concurrency_limit + 1
        # parition. One extra parition will be kept in threadpool task queue.

        log_count = 0
        while True:
            # Check if sigterm was recived for gracefull termination
            if not killer.kill_now:
                # Fetch a request from the database.
                request = fetch_request_from_db()
            else:
                logger.warning('SIGTERM recieved. Stopping futher requets processing.')
                break

            if request is not None:
                executor.submit(make_fetch_request, request, error_map)
            else:
                logger.info("No request available. Waiting...")
                time.sleep(30)

            # Each license should not pick more partitions than it's
            # concurrency_limit. We limit the threadpool queue size to just 1
            # to prevent the license from picking more partitions than
            # it's concurrency_limit. When an executor is freed up, the task
            # in queue is picked and license fetches another task.
            while executor._work_queue.qsize() >= 1:
                # To prevent flooding of this log, we log this every 60 seconds.
                if log_count%60 == 0:
                    logger.info("Worker busy. Waiting...")
                # Reset log_count if it goes beyond 3600.
                log_count = 1 if log_count >= 3600 else log_count + 1
                time.sleep(1)

        logger.warning('Graceful Termination. Waiting for remaining requests to complete.')
        # Making sure all pending requests are completed 
        executor.shutdown(wait=True)
        logger.warning('Graceful Termination. Completed all pending requests.')


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
    try:
        license = sys.argv[2]

        logger.info(f"Deployment for license: {license}.")
        boot_up(license)
        main()
    except Exception as e:
        logger.info(f"License error: {e}.")
        raise e

    logger.info('License deployment shutting down.')
