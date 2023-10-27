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
import json
from fastapi.testclient import TestClient
from main import app
from database.download_handler import get_download_handler, get_mock_download_handler
from database.license_handler import get_license_handler, get_mock_license_handler
from routers.license import (
    get_create_deployment,
    get_create_deployment_mock,
    get_terminate_license_deployment,
    get_terminate_license_deployment_mock,
)
from database.queue_handler import get_queue_handler, get_mock_queue_handler

client = TestClient(app)

logger = logging.getLogger(__name__)

app.dependency_overrides[get_download_handler] = get_mock_download_handler
app.dependency_overrides[get_license_handler] = get_mock_license_handler
app.dependency_overrides[get_queue_handler] = get_mock_queue_handler
app.dependency_overrides[get_create_deployment] = get_create_deployment_mock
app.dependency_overrides[
    get_terminate_license_deployment
] = get_terminate_license_deployment_mock


def _get_license(headers, query, code, expected):
    response = client.get("/license", headers=headers, params=query)

    assert response.status_code == code
    assert response.json() == expected


def test_get_license_basic():
    headers = {}
    query = {}
    code = 200
    expected = [{
        "license_id": "L1",
        "secret_id": "xxxx",
        "client_name": "dummy_client",
        "k8s_deployment_id": "k1",
        "number_of_requets": 100,
    }]

    _get_license(headers, query, code, expected)


def test_get_license_client_name():
    headers = {}
    client_name = "dummy_client"
    query = {"client_name": client_name}
    code = 200
    expected = [{
        "license_id": "L1",
        "secret_id": "xxxx",
        "client_name": client_name,
        "k8s_deployment_id": "k1",
        "number_of_requets": 100,
    }]

    _get_license(headers, query, code, expected)


def _add_license(headers, payload, code, expected):
    response = client.post(
        "/license",
        headers=headers,
        data=json.dumps(payload),
        params={"license_id": "L1"},
    )

    print(f"test add license {response.json()}")

    assert response.status_code == code
    assert response.json() == expected


def test_add_license_basic():
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    license = {
        "license_id": "no-exists",
        "client_name": "dummy_client",
        "number_of_requests": 0,
        "secret_id": "xxxx",
    }
    payload = license
    code = 200
    expected = {"license_id": "L1", "message": "License added successfully."}

    _add_license(headers, payload, code, expected)


def _get_license_by_license_id(headers, license_id, code, expected):
    response = client.get(f"/license/{license_id}", headers=headers)

    logger.info(f"response {response.json()}")
    assert response.status_code == code
    assert response.json() == expected


def test_get_license_by_license_id():
    headers = {"accept": "application/json", "Content-Type": "application/json"}
    license_id = "L1"
    code = 200
    expected = {
        "license_id": license_id,
        "secret_id": "xxxx",
        "client_name": "dummy_client",
        "k8s_deployment_id": "k1",
        "number_of_requets": 100,
    }

    _get_license_by_license_id(headers, license_id, code, expected)


def test_get_license_wrong_license():
    headers = {}
    license_id = "not_exist"
    code = 404
    expected = {
        "detail": "License not_exist not found.",
    }

    _get_license_by_license_id(headers, license_id, code, expected)


def _update_license(headers, license_id, license, code, expected):
    response = client.put(
        f"/license/{license_id}", headers=headers, data=json.dumps(license)
    )

    print(f"_update license {response.json()}")

    assert response.status_code == code
    assert response.json() == expected


def test_update_license_basic():
    headers = {}
    license_id = "L1"
    license = {
        "license_id": "L1",
        "client_name": "dummy_client",
        "number_of_requests": 0,
        "secret_id": "xxxx",
    }
    code = 200
    expected = {"license_id": license_id, "name": "License updated successfully."}

    _update_license(headers, license_id, license, code, expected)


def test_update_license_wrong_license_id():
    headers = {}
    license_id = "no-exists"
    license = {
        "license_id": "no-exists",
        "client_name": "dummy_client",
        "number_of_requests": 0,
        "secret_id": "xxxx",
    }
    code = 404
    expected = {"detail": "No such license no-exists to update."}

    _update_license(headers, license_id, license, code, expected)


def _delete_license(headers, license_id, code, expected):
    response = client.delete(f"/license/{license_id}", headers=headers)

    assert response.status_code == code
    assert response.json() == expected


def test_delete_license_basic():
    headers = {}
    license_id = "L1"
    code = 200
    expected = {"license_id": license_id, "message": "License removed successfully."}

    _delete_license(headers, license_id, code, expected)


def test_delete_license_wrong_license():
    headers = {}
    license_id = "not_exist"
    code = 404
    expected = {"detail": "No such license not_exist to delete."}

    _delete_license(headers, license_id, code, expected)
