# Copyright 2021 Google LLC
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
import calendar
import copy
import copy as cp
import dataclasses
import itertools
import typing as t

Values = t.Union[t.List['Values'], t.Dict[str, 'Values'], bool, int, float, str]  # pytype: disable=not-supported-yet


@dataclasses.dataclass
class Config:
    """Contains pipeline parameters.

    Attributes:
        client:
            Name of the Weather-API-client. Supported clients are mentioned in the 'CLIENTS' variable.
        dataset (optional):
            Name of the target dataset. Allowed options are dictated by the client.
        partition_keys (optional):
            Choose the keys from the selection section to partition the data request.
            This will compute a cartesian cross product of the selected keys
            and assign each as their own download.
        target_path:
            Download artifact filename template. Can make use of Python's standard string formatting.
            It can contain format symbols to be replaced by partition keys;
            if this is used, the total number of format symbols must match the number of partition keys.
        subsection_name:
            Name of the particular subsection. 'default' if there is no subsection.
        force_download:
            Force redownload of partitions that were previously downloaded.
        user_id:
            Username from the environment variables.
        kwargs (optional):
            For representing subsections or any other parameters.
        selection:
            Contains parameters used to select desired data.
    """

    client: str = ""
    dataset: t.Optional[str] = ""
    target_path: str = ""
    partition_keys: t.Optional[t.List[str]] = dataclasses.field(default_factory=list)
    subsection_name: str = "default"
    force_download: bool = False
    user_id: str = "unknown"
    kwargs: t.Optional[t.Dict[str, Values]] = dataclasses.field(default_factory=dict)
    selection: t.Dict[str, Values] = dataclasses.field(default_factory=dict)

    @classmethod
    def from_dict(cls, config: t.Dict) -> 'Config':
        config_instance = cls()
        for section_key, section_value in config.items():
            if section_key == "parameters":
                for key, value in section_value.items():
                    if hasattr(config_instance, key):
                        setattr(config_instance, key, value)
                    else:
                        config_instance.kwargs[key] = value
            if section_key == "selection":
                config_instance.selection = section_value
        return config_instance


def optimize_selection_partition(selection: t.Dict) -> t.Dict:
    """Compute right-hand-side values for the selection section of a single partition.

    Used to support custom syntax and optimizations, such as 'all'.
    """
    selection_ = copy.deepcopy(selection)

    if 'day' in selection_.keys() and selection_['day'] == 'all':
        year, month = selection_['year'], selection_['month']

        multiples_error = "Cannot use keyword 'all' on selections with multiple '{type}'s."

        if isinstance(year, list):
            assert len(year) == 1, multiples_error.format(type='year')
            year = year[0]

        if isinstance(month, list):
            assert len(month) == 1, multiples_error.format(type='month')
            month = month[0]

        if isinstance(year, str):
            assert '/' not in year, multiples_error.format(type='year')

        if isinstance(month, str):
            assert '/' not in month, multiples_error.format(type='month')

        year, month = int(year), int(month)

        _, n_days_in_month = calendar.monthrange(year, month)

        selection_['date'] = f'{year:04d}-{month:02d}-01/to/{year:04d}-{month:02d}-{n_days_in_month:02d}'
        del selection_['day']
        del selection_['month']
        del selection_['year']

    return selection_


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
