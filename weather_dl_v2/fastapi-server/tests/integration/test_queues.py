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


import logging
from main import app
from fastapi.testclient import TestClient
from database.download_handler import get_download_handler, get_mock_download_handler
from database.license_handler import get_license_handler, get_mock_license_handler
from database.queue_handler import get_queue_handler, get_mock_queue_handler

client = TestClient(app)

logger = logging.getLogger(__name__)

app.dependency_overrides[get_download_handler] = get_mock_download_handler
app.dependency_overrides[get_license_handler] = get_mock_license_handler
app.dependency_overrides[get_queue_handler] = get_mock_queue_handler


def _get_all_queue(headers, query, code, expected):
    response = client.get("/queues", headers=headers, params=query)

    assert response.status_code == code
    assert response.json() == expected


def test_get_all_queues():
    headers = {}
    query = {}
    code = 200
    expected = [{"client_name": "dummy_client", "license_id": "L1", "queue": []}]

    _get_all_queue(headers, query, code, expected)


def test_get_client_queues():
    headers = {}
    client_name = "dummy_client"
    query = {"client_name": client_name}
    code = 200
    expected = [{"client_name": client_name, "license_id": "L1", "queue": []}]

    _get_all_queue(headers, query, code, expected)


def _get_queue_by_license(headers, license_id, code, expected):
    response = client.get(f"/queues/{license_id}", headers=headers)

    assert response.status_code == code
    assert response.json() == expected


def test_get_queue_by_license_basic():
    headers = {}
    license_id = "L1"
    code = 200
    expected = {"client_name": "dummy_client", "license_id": license_id, "queue": []}

    _get_queue_by_license(headers, license_id, code, expected)


def test_get_queue_by_license_wrong_license():
    headers = {}
    license_id = "no_exists"
    code = 404
    expected = {"detail": 'License priority for no_exists not found.'}

    _get_queue_by_license(headers, license_id, code, expected)


def _modify_license_queue(headers, license_id, priority_list, code, expected):
    response = client.post(f"/queues/{license_id}", headers=headers, data=priority_list)

    assert response.status_code == code
    assert response.json() == expected


def test_modify_license_queue_basic():
    headers = {}
    license_id = "L1"
    priority_list = []
    code = 200
    expected = {"message": f"'{license_id}' license priority updated successfully."}

    _modify_license_queue(headers, license_id, priority_list, code, expected)


def test_modify_license_queue_wrong_license_id():
    headers = {}
    license_id = "no_exists"
    priority_list = []
    code = 404
    expected = {"detail": 'License no_exists not found.'}

    _modify_license_queue(headers, license_id, priority_list, code, expected)


def _modify_config_priority_in_license(headers, license_id, query, code, expected):
    response = client.put(f"/queues/priority/{license_id}", params=query)

    logger.info(f"response {response.json()}")

    assert response.status_code == code
    assert response.json() == expected


def test_modify_config_priority_in_license_basic():
    headers = {}
    license_id = "L1"
    query = {"config_name": "example.cfg", "priority": 0}
    code = 200
    expected = {
        "message": f"'{license_id}' license -- 'example.cfg' priority updated successfully."
    }

    _modify_config_priority_in_license(headers, license_id, query, code, expected)


def test_modify_config_priority_in_license_wrong_license():
    headers = {}
    license_id = "no_exists"
    query = {"config_name": "example.cfg", "priority": 0}
    code = 404
    expected = {"detail": 'License no_exists not found.'}

    _modify_config_priority_in_license(headers, license_id, query, code, expected)


def test_modify_config_priority_in_license_wrong_config():
    headers = {}
    license_id = "no_exists"
    query = {"config_name": "wrong.cfg", "priority": 0}
    code = 404
    expected = {"detail": 'License no_exists not found.'}

    _modify_config_priority_in_license(headers, license_id, query, code, expected)
