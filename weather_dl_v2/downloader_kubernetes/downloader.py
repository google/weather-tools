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


"""
This program downloads ECMWF data & upload it into GCS.
"""
import tempfile
import os
import sys
import time
from manifest import FirestoreManifest, Stage
from util import copy, download_with_aria2, download_with_wget
import datetime


def download(url: str, path: str) -> None:
    """Download data from client, with retries."""
    if path:
        if os.path.exists(path):
            # Empty the target file, if it already exists, otherwise the
            # transfer below might be fooled into thinking we're resuming
            # an interrupted download.
            open(path, "w").close()

        download_methods = [download_with_aria2, download_with_aria2, download_with_wget]
        errors = []

        for method in download_methods:
            print(f"Trying {method.__name__}.")
            try:
                method(url, path)
                return
            except Exception as e:
                print(f"{method.__name__} failed. Error: {e}.")
                errors.append(str(e))
                print(f"Waiting for 2 mins.")
                time.sleep(120)

        err_msgs = '\n'.join(errors)
        print(f"Failed to download {url}. Error Msg: {err_msgs}.")
        raise Exception(f"Downloading failed for url {url} & path {path}.\nError Msg: {err_msgs}.")


def main(
    config_name, dataset, selection, user_id, url, target_path, license_id
) -> None:
    """Download data from a client to a temp file."""

    manifest = FirestoreManifest(license_id=license_id)
    temp_name = ""
    with manifest.transact(config_name, dataset, selection, target_path, user_id):
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            temp_name = temp.name
            manifest.set_stage(Stage.DOWNLOAD)
            precise_download_start_time = (
                datetime.datetime.utcnow()
                .replace(tzinfo=datetime.timezone.utc)
                .isoformat(timespec="seconds")
            )
            manifest.prev_stage_precise_start_time = precise_download_start_time
            print(f"Downloading data for {target_path!r}.")
            download(url, temp_name)
            print(f"Download completed for {target_path!r}.")

            manifest.set_stage(Stage.UPLOAD)
            precise_upload_start_time = (
                datetime.datetime.utcnow()
                .replace(tzinfo=datetime.timezone.utc)
                .isoformat(timespec="seconds")
            )
            manifest.prev_stage_precise_start_time = precise_upload_start_time
            print(f"Uploading to store for {target_path!r}.")
            copy(temp_name, target_path)
            print(f"Upload to store complete for {target_path!r}.")
    os.unlink(temp_name)


if __name__ == "__main__":
    main(*sys.argv[1:])
