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


import getpass
import logging
import os
from .parsers import process_config
from .partition import PartitionConfig
from .manifest import FirestoreManifest
from database.download_handler import get_download_handler
from database.queue_handler import get_queue_handler
from fastapi.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)

download_handler = get_download_handler()
queue_handler = get_queue_handler()


def _do_partitions(partition_obj: PartitionConfig):
    for partition in partition_obj.prepare_partitions():
        # Skip existing downloads
        if partition_obj.new_downloads_only(partition):
            partition_obj.update_manifest_collection(partition)


# TODO: Make partitioning faster.
async def start_processing_config(config_file, licenses, force_download, priority = None):
    config = {}
    manifest = FirestoreManifest()

    with open(config_file, "r", encoding="utf-8") as f:
        # configs/example.cfg -> example.cfg
        config_name = os.path.split(config_file)[1]
        config = process_config(f, config_name)

    config.force_download = force_download
    config.user_id = getpass.getuser()

    partition_obj = PartitionConfig(config, None, manifest)

    # Make entry in 'download' & 'queues' collection.
    await download_handler._start_download(config_name, config.client)
    await download_handler._mark_partitioning_status(
        config_name, "Partitioning in-progress."
    )
    try:
        # Prepare partitions
        await run_in_threadpool(_do_partitions, partition_obj)
        await download_handler._mark_partitioning_status(
            config_name, "Partitioning completed."
        )
        for license_id in licenses:
            await queue_handler._update_config_priority_in_license(license_id, config_name, priority)
    except Exception as e:
        error_str = f"Partitioning failed for {config_name} due to {e}."
        logger.error(error_str)
        await download_handler._mark_partitioning_status(config_name, error_str)
