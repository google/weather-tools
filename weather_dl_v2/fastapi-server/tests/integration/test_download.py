import logging
import os
from fastapi.testclient import TestClient
from main import app, ROOT_DIR
from database.download_handler import get_download_handler, get_mock_download_handler
from database.license_handler import get_license_handler, get_mock_license_handler
from database.queue_handler import get_queue_handler, get_mock_queue_handler

from database.queue_handler import get_queue_handler

client = TestClient(app)

logger = logging.getLogger(__name__)

app.dependency_overrides[get_download_handler] = get_mock_download_handler
app.dependency_overrides[get_license_handler] = get_mock_license_handler
app.dependency_overrides[get_queue_handler] = get_mock_queue_handler


def _get_download(headers, query, code, expected):
    response = client.get("/download", headers=headers, params=query)
    
    assert response.status_code == code
    assert response.json() == expected
    

def test_get_downloads_basic():
    headers = {}
    query = {}
    code = 200
    expected = [
        {
            "config_name": "config_1",
            "client_name": "MARS",
            "total_shards": 10000,
            "scheduled_shards": 4990,
            "downloaded_shards": 5000,
            "failed_shards": 0
        },
        {
            "config_name": "config_2",
            "client_name": "MARS",
            "total_shards": 10000,
            "scheduled_shards": 4990,
            "downloaded_shards": 5000,
            "failed_shards": 0
        },
        {
            "config_name": "config_3",
            "client_name": "CDS",
            "total_shards": 10000,
            "scheduled_shards": 4990,
            "downloaded_shards": 5000,
            "failed_shards": 0
        }
    ]

    _get_download(headers, query, code, expected)


def _submit_download(headers, file_path, licenses, code, expected):
    file = None
    try:
        file = {"file" : open(file_path, 'rb')}
    except FileNotFoundError:
        print("file not found.")

    payload = {"licenses" : licenses}

    response = client.post("/download", headers=headers, files=file, data=payload)

    print(f"resp {response.json()}")

    assert response.status_code == code
    assert response.json() == expected

def test_submit_download_basic():
    header = {
        "accept": "application/json",
    }
    file_path = os.path.join(ROOT_DIR, "tests/test_data/no_exist.cfg")
    licenses = ["L1"]
    code = 200
    expected = {'message': "file 'no_exist.cfg' saved at './config_files/no_exist.cfg' "'successfully.'}

    _submit_download(header, file_path, licenses, code, expected)

def test_submit_download_file_not_uploaded():
    header = {
        "accept": "application/json",
    }
    file_path = os.path.join(ROOT_DIR, "tests/test_data/wrong_file.cfg")
    licenses = ["L1"]
    code = 404
    expected = {"detail": "No upload file sent."}

    _submit_download(header, file_path, licenses, code, expected)

def test_submit_download_file_alreadys_exist():
    header = {
        "accept": "application/json",
    }
    file_path = os.path.join(ROOT_DIR, "tests/test_data/example.cfg")
    licenses = ["L1"]
    code = 400
    expected = {"detail": "Please stop the ongoing download of the config file 'example.cfg' before attempting to start a new download."}

    _submit_download(header, file_path, licenses, code, expected)


def _get_download_by_config(headers, config_name, code, expected):
    response = client.get(f"/download/{config_name}", headers=headers)

    assert response.status_code == code
    assert response.json() == expected

def test_get_download_by_config_basic():
    headers = {}
    config_name = "dummy_config"
    code = 200
    expected = {"config_name": config_name, "client_name": "MARS", "total_shards": 10000, "scheduled_shards": 4990,
              "downloaded_shards": 5000, "failed_shards": 0}
    
    _get_download_by_config(headers, config_name, code, expected)

def test_get_download_by_config_wrong_config():
    headers = {}
    config_name = "no_exist"
    code = 404
    expected = {'detail': 'Download config not found in weather-dl v2.'}

    _get_download_by_config(headers, config_name, code, expected)

def _delete_download_by_config(headers, config_name, code, expected):
    response = client.delete(f"/download/{config_name}",headers=headers)

    assert response.status_code == code
    assert response.json() == expected
    

def test_delete_download_by_config_basic():
    headers = {}
    config_name = "dummy_config"
    code = 200
    expected = {'config_name': 'dummy_config', 'message': 'Download config stopped & removed successfully.'}

    _delete_download_by_config(headers, config_name, code, expected)

def test_delete_download_by_config_wrong_config():
    headers = {}
    config_name = "no_exist"
    code = 404
    expected = {'detail': 'No such download config to stop & remove.'}

    _delete_download_by_config(headers, config_name, code, expected)
