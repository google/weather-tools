import copy as cp
import dataclasses
import itertools
import typing as t

from .manifest import Manifest
from .parsers import prepare_target_name
from .config import Config
from .stores import Store, FSStore


@dataclasses.dataclass
class PartitionConfig():
    """Partition a config into multiple data requests.

    Partitioning involves four main operations: First, we fan-out shards based on
    partition keys (a cross product of the values). Second, we filter out existing
    downloads (unless we want to force downloads). Last, we assemble each partition
    into a single Config.

    Attributes:
        store: A cloud storage system, used for checking the existence of downloads.
        manifest: A download manifest to register preparation state.
    """

    config: Config
    store: Store
    manifest: Manifest

    def _create_partition_config(self, option: t.Tuple) -> Config:
        """Create a config for a single partition option.

        Output a config dictionary, overriding the range of values for
        each key with the partition instance in 'selection'.
        Continuing the example from prepare_partitions, the selection section
        would be:
        { 'foo': ..., 'year': ['2020'], 'month': ['01'], ... }
        { 'foo': ..., 'year': ['2020'], 'month': ['02'], ... }
        { 'foo': ..., 'year': ['2020'], 'month': ['03'], ... }

        Args:
            option: A single item in the range of partition_keys.
            config: The download config, including the parameters and selection sections.

        Returns:
            A configuration with that selects a single download partition.
        """
        copy = cp.deepcopy(self.config.selection)
        out = cp.deepcopy(self.config)
        for idx, key in enumerate(self.config.partition_keys):
            copy[key] = [option[idx]]

        out.selection = copy
        return out


    def skip_partition(self, config: Config) -> bool:
        """Return true if partition should be skipped."""

        if config.force_download:
            return False

        target = prepare_target_name(config)
        if self.store.exists(target):
            print(f'file {target} found, skipping.')
            self.manifest.skip(config.config_name, config.dataset, config.selection, target, config.user_id)
            return True

        return False


    def prepare_partitions(self) -> t.Iterator[Config]:
        """Iterate over client parameters, partitioning over `partition_keys`.

        This produces a Cartesian-Cross over the range of keys.

        For example, if the keys were 'year' and 'month', it would produce
        an iterable like:
            ( ('2020', '01'), ('2020', '02'), ('2020', '03'), ...)

        Returns:
            An iterator of `Config`s.
        """
        for option in itertools.product(*[self.config.selection[key] for key in self.config.partition_keys]):
            yield self._create_partition_config(option)


    def new_downloads_only(self, candidate: Config) -> bool:
        """Predicate function to skip already downloaded partitions."""
        if self.store is None:
            self.store = FSStore()
        should_skip = self.skip_partition(candidate)
        if should_skip:
            print("Skipped.")
        return not should_skip


    def update_manifest_collection(self, partition: Config) -> Config:
        """Updates the DB."""
        location = prepare_target_name(partition)
        self.manifest.schedule(partition.config_name, partition.dataset, partition.selection, location, partition.user_id)
        print(f'Created partition {location!r}.')