import getpass
import os
from .parsers import process_config
from .partition import PartitionConfig
from .manifest import FirestoreManifest
from db_service.database import FirestoreClient

db_client = FirestoreClient()
    
def start_processing_config(config_file, licenses):
    config = {}
    # TODO: Make use of db_service instead of FirestoreManifest.
    manifest_location = "XXXXXXXXX"
    manifest = FirestoreManifest(manifest_location)

    with open(config_file, 'r', encoding='utf-8') as f:
        # configs/example.cfg -> example.cfg
        config_name = os.path.split(config_file)[1]
        config = process_config(f, config_name)
    
    config.force_download = True
    config.user_id = getpass.getuser()
    
    partition_obj = PartitionConfig(config, None, manifest)

    # Prepare partitions
    for partition in partition_obj.prepare_partitions():
        # Skip existing downloads
        if partition_obj.new_downloads_only(partition):
            partition_obj.update_manifest_collection(partition)
    
    # Make entry in 'download' & 'queues' collection.
    db_client._start_download(config_name, config.client)
    db_client._update_queues_on_start_download(config_name, licenses)
