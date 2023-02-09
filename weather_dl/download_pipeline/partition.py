# Copyright 2022 Google LLC
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
import copy as cp
import dataclasses
import itertools
import logging
import math
import typing as t

import apache_beam as beam

from .config import Config
from .manifest import MANIFESTS, Location, NoOpManifest
from .parsers import prepare_target_name
from .stores import Store, FSStore
from .util import ichunked

Partition = t.Tuple[str, t.Dict, Config]
Index = t.Tuple[int]

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class PartitionConfig(beam.PTransform):
    """Partition a config into multiple data requests.

    Partitioning involves four main operations: First, we fan-out shards based on
    partition keys (a cross product of the values). Second, we filter out existing
    downloads (unless we want to force downloads). Next, we add subsections to the
    configs in a cycle (to ensure an even distribution of extra parameters). Last,
    We assemble each partition into a single Config.

    Attributes:
        store: A cloud storage system, used for checking the existence of downloads.
        subsections: A cycle of (name, parameter) tuples.
        manifest: A download manifest to register preparation state.
        scheduling: How to sort partitions from multiple configs: in order of each
           config (default), or in "fair" order, where partitions from each config
           are evenly rotated.
        partition_chunks: The size of chunks of partition shards to use during the
            fan-out stage. By default, this operation will aim to divide all the
            partitions into groups of about 1000 sized-chunks.
        num_groups: The the number of groups (for fair scheduling). Default: 1.
    """

    store: Store
    subsections: itertools.cycle
    manifest: t.Dict[str, Location]
    scheduling: str
    partition_chunks: t.Optional[int] = None
    num_groups: int = 1

    def expand(self, configs):
        def loop_through_subsections(it: Config) -> Partition:
            """Assign a subsection to each config in a loop.

            If the `parameters` section contains subsections (e.g. '[parameters.1]',
            '[parameters.2]'), collect a repeating cycle of the subsection key-value
            pairs. Otherwise, assign a default section to each config.

            This is useful for specifying multiple API keys for your configuration.

            For example:
            ```
              [parameters.alice]
              api_key=KKKKK1
              api_url=UUUUU1
              [parameters.bob]
              api_key=KKKKK2
              api_url=UUUUU2
              [parameters.eve]
              api_key=KKKKK3
              api_url=UUUUU3
            ```
            """
            name, params = next(self.subsections)
            return name, params, it

        if self.scheduling == 'fair':
            config_idxs = (
                    configs
                    | beam.combiners.ToList()
                    | 'Fair Fan-out' >> beam.FlatMap(prepare_fair_partition_index,
                                                     chunk_size=self.partition_chunks,
                                                     groups=self.num_groups)
            )
        else:
            config_idxs = (
                    configs
                    | 'Fan-out' >> beam.FlatMap(prepare_partition_index, chunk_size=self.partition_chunks)
            )

        return (
                config_idxs
                | beam.Reshuffle()
                | 'To configs' >> beam.FlatMapTuple(prepare_partitions_from_index)
                | 'Skip existing' >> beam.Filter(new_downloads_only, store=self.store)
                | 'Cycle subsections' >> beam.Map(loop_through_subsections)
                | 'Assemble' >> beam.Map(assemble_config, manifest=self.manifest)
        )


def _create_partition_config(option: t.Tuple, config: Config) -> Config:
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
    copy = cp.deepcopy(config.selection)
    out = cp.deepcopy(config)
    for idx, key in enumerate(config.partition_keys):
        copy[key] = [option[idx]]

    out.selection = copy
    return out


def skip_partition(config: Config, store: Store) -> bool:
    """Return true if partition should be skipped."""

    if config.force_download:
        return False

    target = prepare_target_name(config)
    if store.exists(target):
        logger.info(f'file {target} found, skipping.')
        return True

    return False


def prepare_partition_index(config: Config,
                            chunk_size: t.Optional[int] = None) -> t.Iterator[t.Tuple[Config, t.List[Index]]]:
    """Produce indexes over client parameters, partitioning over `partition_keys`

    This produces a Cartesian-Cross over the range of keys.

    For example, if the keys were 'year' and 'month', it would produce
    an iterable like:
        ( (0, 0), (0, 1), (0, 2), ...)

    After the indexes were converted back to keys, it would produce values like:
        ( ('2020', '01'), ('2020', '02'), ('2020', '03'), ...)

    Returns:
        An iterator of index tuples.
    """
    dims = [range(len(config.selection[key])) for key in config.partition_keys]
    n_partitions = math.prod([len(d) for d in dims])
    logger.info(f'Creating {n_partitions} partitions.')

    if chunk_size is None:
        chunk_size = 1000

    for option_idx in ichunked(itertools.product(*dims), chunk_size):
        yield config, list(option_idx)


def prepare_partitions_from_index(config: Config, indexes: t.List[Index]) -> t.Iterator[Config]:
    """Convert a partition index into a config.

    Returns: from an option index.
        A partition `Config` from an option index.
    """
    for index in indexes:
        option = tuple(
            config.selection[config.partition_keys[key_idx]][val_idx] for key_idx, val_idx in enumerate(index)
        )
        yield _create_partition_config(option, config)


def new_downloads_only(candidate: Config, store: t.Optional[Store] = None) -> bool:
    """Predicate function to skip already downloaded partitions."""
    if store is None:
        store = FSStore()
    should_skip = skip_partition(candidate, store)
    if should_skip:
        beam.metrics.Metrics.counter('Prepare', 'skipped').inc()
    return not should_skip


def assemble_config(partition: Partition, manifest: t.Dict[str, Location]) -> Config:
    """Assemble the configuration for a single partition.

    For each cross product of the 'selection' sections, the output dictionary
    will overwrite parameters from the extra param subsections, evenly cycling
    through each subsection.

    For example:
      { 'parameters': {... 'api_key': KKKKK1, ... }, ... }
      { 'parameters': {... 'api_key': KKKKK2, ... }, ... }
      { 'parameters': {... 'api_key': KKKKK3, ... }, ... }
      { 'parameters': {... 'api_key': KKKKK1, ... }, ... }
      { 'parameters': {... 'api_key': KKKKK2, ... }, ... }
      { 'parameters': {... 'api_key': KKKKK3, ... }, ... }
      ...

    Returns:
        An `Config` assembled out of subsection parameters and config shards.
    """
    manifest_obj = MANIFESTS.get(manifest['type'], NoOpManifest)(manifest['location'])
    name, params, out = partition
    out.kwargs.update(params)
    out.subsection_name = name

    location = prepare_target_name(out)
    user = out.user_id
    manifest_obj.schedule(out.selection, location, user)

    logger.info(f'[{name}] Created partition {location!r}.')
    beam.metrics.Metrics.counter('Subsection', name).inc()

    return out


def cycle_iters(iters: t.List[t.Iterator], take: int = 1) -> t.Iterator:
    """Evenly cycle through a list of iterators.

    Args:
        iters: A list of iterators to evely cycle through.
        take: Yield N items at a time. When not set to 1, this will yield
          multiple items from the same collection.

    Returns:
        An iteration across several iterators in a round-robin order.
    """
    while iters:
        for i, it in enumerate(iters):
            try:
                for j in range(take):
                    logger.debug(f'yielding item {j!r} from iterable {i!r}.')
                    yield next(it)
            except StopIteration:
                iters.remove(it)


def prepare_fair_partition_index(configs: t.List[Config],
                                 chunk_size: t.Optional[int],
                                 groups: int) -> t.Iterator[t.Tuple[Config, t.List[Index]]]:
    """Given a list of all configs, evenly cycle through each partition chunked by the 'chunk_size'."""
    if chunk_size is None:
        chunk_size = 1
    iters = [prepare_partition_index(config, chunk_size) for config in configs]
    yield from cycle_iters(iters, take=groups)
