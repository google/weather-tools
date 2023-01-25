# -*- coding: utf-8 -*-
"""
This program downloads ECMWF data & upload it into GCS.
"""
import tempfile
import subprocess
import os
import sys
from manifest import FirestoreManifest

# [START gke_pubsub_pull]
# [START container_pubsub_pull]
def copy(src: str, dst: str) -> None:
    """Copy data via `gsutil cp`."""
    try:
        subprocess.run(['gsutil', 'cp', src, dst], check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        print(f'Failed to copy file {src!r} to {dst!r} due to {e.stderr.decode("utf-8")}')
        raise


def download(url: str, path: str) -> None:
    """Download data from client, with retries."""
    if path:
        if os.path.exists(path):
            # Empty the target file, if it already exists, otherwise the
            # transfer below might be fooled into thinking we're resuming
            # an interrupted download.
            open(path, "w").close()
        dir_path, file_name = os.path.split(path)
        try:
            subprocess.run(
                ['aria2c', '-x', '16', '-s', '16', url, '-d', dir_path, '-o', file_name, '--allow-overwrite'],
                check=True,
                capture_output=True)
        except subprocess.CalledProcessError as e:
            print(f'Failed download from ECMWF server {url!r} to {path!r} due to {e.stderr.decode("utf-8")}')


def main(selection, user_id, url, target_path) -> None:
    """Download data from a client to a temp file."""

    manifest_location = "XXXXXXXXXX"
    manifest = FirestoreManifest(manifest_location)
    temp_name = ""
    with manifest.transact(selection, target_path, user_id, 'download'):
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            temp_name = temp.name
            print(f'Downloading data for {target_path!r}.')
            download(url, temp_name)
            print(f'Download completed for {target_path!r}.')
    with manifest.transact(selection, target_path, user_id, 'upload'):
            print(f'Uploading to store for {target_path!r}.')
            copy(temp_name, target_path)
            print(f'Upload to store complete for {target_path!r}.')
    os.unlink(temp_name)

# [END container_pubsub_pull]
# [END gke_pubsub_pull]

if __name__ == '__main__':
    temp_args = sys.argv
    main(temp_args[1], temp_args[2], temp_args[3], temp_args[4])
