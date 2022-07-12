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
import typing as t
import numpy as np
import pandas as pd
from .clients import CLIENTS

import apache_beam as beam

from .manifest import Manifest
from .parsers import prepare_target_name
from .config import Config
from .stores import Store, FSStore

Partition = t.Tuple[str, t.Dict, Config]

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
    """

    store: Store
    subsections: itertools.cycle
    manifest: Manifest
    config_client: str

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

        if self.config_client == 'eumetsat':
            prepared_partitions = (
                configs
                | 'SplitTimes' >> beam.FlatMap(time_slices)
                | 'Reshuffle time slices' >> beam.Reshuffle()
                | 'Prepare partions' >> beam.FlatMap(prepare_partitions_for_eumetsat, next(self.subsections))
            )
        else:
            prepared_partitions = (
                configs
                | 'Prepare partions' >> beam.FlatMap(prepare_partitions)
            )
        return (
                prepared_partitions
                | 'Skip existing downloads' >> beam.Filter(new_downloads_only, store=self.store)
                | 'Cycle through subsections' >> beam.Map(loop_through_subsections)
                | 'Assemble the data request' >> beam.Map(assemble_config, manifest=self.manifest)
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


def _split_dates_to_months(start: str, end: str) -> np.ndarray:
    slices = pd.date_range(start=start, end=end, freq='30D')
    logger.info('slices=%s', slices)
    return np.append(slices, pd.to_datetime(end).asm8)


def time_slices(config: Config) -> t.Iterable[t.Tuple[Config, np.datetime64, np.datetime64]]:
    dates = _split_dates_to_months(config.selection['start_date'], config.selection['end_date'])
    logger.info('dates=%s', dates)
    for s, e in zip(dates[:-1], dates[1:]):
        yield config, s, e


def prepare_partitions(config: Config) -> t.Iterator[Config]:
    """Iterate over client parameters, partitioning over `partition_keys`.

    This produces a Cartesian-Cross over the range of keys.

    For example, if the keys were 'year' and 'month', it would produce
    an iterable like:
        ( ('2020', '01'), ('2020', '02'), ('2020', '03'), ...)

    Returns:
        An iterator of `Config`s.
    """
    for option in itertools.product(*[config.selection[key] for key in config.partition_keys]):
        yield _create_partition_config(option, config)


def prepare_partitions_for_eumetsat(element: t.Tuple[Config, np.datetime64, np.datetime64], subsection) -> t.Iterator[
                                                                                                            Config]:
    """Iterate over client parameters, partitioning over `partition_keys`.

    This fetches the product_ids and consider each of them as single partition.

    Returns:
        An iterator of `Config`s.
    """
    config, start, end = element
    client = CLIENTS[config.client](config, config_subsection=subsection)
    for option in client.products(start, end, config.dataset):
        yield _create_partition_config((option, ), config)


def new_downloads_only(candidate: Config, store: t.Optional[Store] = None) -> bool:
    """Predicate function to skip already downloaded partitions."""
    if store is None:
        store = FSStore()
    should_skip = skip_partition(candidate, store)
    if should_skip:
        beam.metrics.Metrics.counter('Prepare', 'skipped').inc()
    return not should_skip


def assemble_config(partition: Partition, manifest: Manifest) -> Config:
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
    name, params, out = partition
    out.kwargs.update(params)
    out.subsection_name = name

    location = prepare_target_name(out)
    user = out.user_id
    manifest.schedule(out.selection, location, user)

    logger.info(f'[{name}] Created partition {location!r}.')
    beam.metrics.Metrics.counter('Subsection', name).inc()

    return out
