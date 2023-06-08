import json
import threading
import time
import sys
import os

from database import FirestoreClient
from job_creator import create_download_job
from clients import CLIENTS
from manifest import FirestoreManifest

db_client = FirestoreClient()

def create_job(request, result):
    res = {
          'config_name': request['config_name'],
          'dataset': request['dataset'],
          'selection': json.loads(request['selection']),
          'user_id': request['username'],
          'url': result['href'],
          'target_path': request['location']
          }

    data_str = json.dumps(res)
    create_download_job(data_str)


def make_fetch_request(request):
    with semaphore:
        client = CLIENTS[client_name](request['dataset'])
        manifest = FirestoreManifest()
        print(f'By using {client_name} datasets, '
              f'users agree to the terms and conditions specified in {client.license_url!r}')

        target = request['location']
        selection = json.loads(request['selection'])

        print(f'Fetching data for {target!r}.')
        with manifest.transact(request['config_name'], request['dataset'], selection, target, request['username']):
            result = client.retrieve(request['dataset'], selection, manifest)
        print(f"Result fetched {result} for request {request}.")
        create_job(request, result)


def fetch_request_from_db():
    request = None
    config_name = db_client._get_config_from_queue_by_license_id(license_id)
    if config_name:
        request = db_client._get_partition_from_manifest(config_name)
        if not request:
            db_client._remove_config_from_license_queue(license_id, config_name)
    return request


def main():
    print("Started looking at the request.")
    while True:
        # Fetch a request from the database
        request = fetch_request_from_db()

        if request is not None:
            # Create a thread to process the request
            thread = threading.Thread(target=make_fetch_request, args=(request,))
            thread.start()
        else:
            print("No request available. Waiting...")
            time.sleep(5)

        # Check if the maximum concurrency level has been reached
        # If so, wait for a slot to become available
        with semaphore:
            pass


def boot_up(license: str) -> None:
    global license_id, client_name, concurrency_limit, semaphore

    result = db_client._initialize_license_deployment(license)
    license_id = license
    client_name = result['client_name']
    concurrency_limit = result['number_of_requests']
    os.environ.setdefault('CLIENT_URL', result['api_url'])
    os.environ.setdefault('CLIENT_KEY', result['api_key'])
    os.environ.setdefault('CLIENT_EMAIL', result['api_email'])
    semaphore = threading.Semaphore(concurrency_limit)
 

if __name__ == "__main__":
    license = sys.argv[2]
    print(f"Deployment for license: {license}.")
    boot_up(license)
    main()
