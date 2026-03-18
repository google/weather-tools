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
import dataclasses
import itertools
import typing as t

Values = t.Union[t.List['Values'], t.Dict[str, 'Values'], bool, int, float, str]  # pytype: disable=not-supported-yet


@dataclasses.dataclass
class Config:
    """Contains pipeline parameters.

    Attributes:
        config_name:
            Name of the config file.
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

    config_name: str = ""
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

    if 'date_range' in selection_.keys():
        selection_['date'] = selection_['date_range'][0]
        del selection_['date_range']

    if 'day' in selection_.keys() and selection_['day'] == 'all':
        years, months = selection_['year'], selection_['month']

        multiples_error = "/ is not allowed in {type}."

        if isinstance(years, str):
            years = [years]

        if isinstance(months, str):
            months = [months]

        date_ranges = []

        # Generating dates for every year-month.
        for year, month in itertools.product(years, months):

            if isinstance(year, str):
                assert '/' not in year, multiples_error.format(type='year')

            if isinstance(month, str):
                assert '/' not in month, multiples_error.format(type='month')

            year, month = int(year), int(month)

            _, n_days_in_month = calendar.monthrange(year, month)

            date_range = [f'{year:04d}-{month:02d}-{day:02d}' for day in range(1, n_days_in_month + 1)]

            date_ranges.extend(date_range)

        selection_['date'] = date_ranges
        del selection_['day']
        del selection_['month']
        del selection_['year']

    return selection_
