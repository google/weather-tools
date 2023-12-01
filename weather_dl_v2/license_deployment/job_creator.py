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


from os import path
import yaml
import json
import uuid
from kubernetes import client, config
from deployment_config import get_config


def create_download_job(message):
    """Creates a kubernetes workflow of type Job for downloading the data."""
    parsed_message = json.loads(message)
    (
        config_name,
        dataset,
        selection,
        user_id,
        url,
        target_path,
        license_id,
    ) = parsed_message.values()
    selection = str(selection).replace(" ", "")
    config.load_config()

    with open(path.join(path.dirname(__file__), "downloader.yaml")) as f:
        dep = yaml.safe_load(f)
        uid = uuid.uuid4()
        dep["metadata"]["name"] = f"downloader-job-id-{uid}"
        dep["spec"]["template"]["spec"]["containers"][0]["command"] = [
            "python",
            "downloader.py",
            config_name,
            dataset,
            selection,
            user_id,
            url,
            target_path,
            license_id,
        ]
        dep["spec"]["template"]["spec"]["containers"][0][
            "image"
        ] = get_config().downloader_k8_image
        batch_api = client.BatchV1Api()
        batch_api.create_namespaced_job(body=dep, namespace="default")
