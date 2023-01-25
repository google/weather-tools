import getpass
from parsers import (
    process_config,
    get_subsections
)
from fetch import reader
from partition import PartitionConfig
import itertools

async def start_processing_config(config_file):
    config = {}
    with open(config_file, 'r', encoding='utf-8') as f:
        config = process_config(f)
    
    config.force_download = True
    config.user_id = getpass.getuser()

    subsections = get_subsections(config)
    
    subsections_cycle = itertools.cycle(subsections)
    
    partition_obj = PartitionConfig(config, None, subsections_cycle)
    
    final_partition_list = []
    # Prepare partitions
    for partition in partition_obj.prepare_partitions():
        # Skip existing downloads
        if partition_obj.new_downloads_only(partition):
            # Cycle through subsections
            _partition = partition_obj.loop_through_subsections(partition)
            # Assemble the data request
            final_partition_list.append(partition_obj.assemble_config(_partition))

    await reader(final_partition_list)
