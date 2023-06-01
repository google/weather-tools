# -*- coding: utf-8 -*-
"""
This program downloads ECMWF data & upload it into GCS.
"""
import tempfile
import subprocess
import os
import sys
from manifest import FirestoreManifest, Stage, ConsoleManifest
from util import copy, download_with_aria2
import datetime

# [START gke_pubsub_pull]
# [START container_pubsub_pull]
def download(url: str, path: str) -> None:
    """Download data from client, with retries."""
    if path:
        if os.path.exists(path):
            # Empty the target file, if it already exists, otherwise the
            # transfer below might be fooled into thinking we're resuming
            # an interrupted download.
            open(path, "w").close()
        download_with_aria2(url, path)


def main(config_name, dataset, selection, user_id, url, target_path) -> None:
    """Download data from a client to a temp file."""
    # fs://<collection-name>?projectId=<project-id>`
    manifest_location = "XXXXXXXXXX"
    manifest = FirestoreManifest(manifest_location)
    # manifest = ConsoleManifest('cli://manifest')
    temp_name = ""
    with manifest.transact(config_name, dataset, selection, target_path, user_id):
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            temp_name = temp.name
            manifest.set_stage(Stage.DOWNLOAD)
            precise_download_start_time = (
                datetime.datetime.utcnow()
                .replace(tzinfo=datetime.timezone.utc)
                .isoformat(timespec='seconds')
            )
            manifest.prev_stage_precise_start_time = precise_download_start_time
            print(f'Downloading data for {target_path!r}.')
            download(url, temp_name)
            print(f'Download completed for {target_path!r}.')

            manifest.set_stage(Stage.UPLOAD)
            precise_upload_start_time = (
                datetime.datetime.utcnow()
                .replace(tzinfo=datetime.timezone.utc)
                .isoformat(timespec='seconds')
            )
            manifest.prev_stage_precise_start_time = precise_upload_start_time
            print(f'Uploading to store for {target_path!r}.')
            copy(temp_name, target_path)
            print(f'Upload to store complete for {target_path!r}.')
    os.unlink(temp_name)
# [END container_pubsub_pull]
# [END gke_pubsub_pull]

if __name__ == '__main__':
    temp_args = sys.argv
    main(temp_args[1], temp_args[2], temp_args[3], temp_args[4], temp_args[5], temp_args[6])
