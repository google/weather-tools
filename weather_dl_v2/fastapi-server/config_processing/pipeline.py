import getpass
import os
from .parsers import process_config
from .partition import PartitionConfig
from .manifest import FirestoreManifest
from firestore_db.db import fake_download_db, fake_license_priority_db

def start_processing_config(config_file, licenses):
    config = {}
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
    
    # Make entry in fake_download_db & fake_license_priority_db as mentioned by user.
    fake_download_db[config_name] = {'client_name': config.client}
    for license in licenses:
        fake_license_priority_db[license].append(config_name)
