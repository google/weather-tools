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


import calendar
import copy
import dataclasses
import datetime
import itertools
import typing as t

Values = t.Union[t.List["Values"], t.Dict[str, "Values"], bool, int, float, str]  # pytype: disable=not-supported-yet


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
    def from_dict(cls, config: t.Dict) -> "Config":
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


def _parse_date_token(token: str) -> datetime.date:
    """Parses a MARS date token into datetime.date."""
    if token.startswith("-") and token[1:].isdecimal():
        return datetime.date.today() - datetime.timedelta(days=int(token[1:]))
    for fmt in ("%Y-%m-%d", "%Y%m%d", "%Y-%j"):
        try:
            return datetime.datetime.strptime(token, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Invalid date token '{token}' in 'date_range'.")


def _expand_date_range(block: str) -> t.List[str]:
    """Expands a MARS date_range string (e.g. 'D1/to/D2[/by/N]') into YYYY-MM-DD dates."""
    tokens = [b.strip() for b in block.split("/") if b.strip()]
    if "to" not in tokens and "by" not in tokens:
        return [_parse_date_token(tok).strftime("%Y-%m-%d") for tok in tokens]
    try:
        to_idx = tokens.index("to")
        assert to_idx != 0, "There must be a start token."
        start = _parse_date_token(tokens[to_idx - 1])
        end = _parse_date_token(tokens[to_idx + 1])
        increment = int(tokens[tokens.index("by") + 1]) if "by" in tokens else 1
        assert increment != 0, "Increment cannot be 0."
    except (AssertionError, IndexError, ValueError) as exc:
        raise SyntaxError(f"Improper range syntax in '{block}'.") from exc
    if start > end and increment > 0:
        increment *= -1
    offset = 1 if start <= end else -1
    return [
        (start + datetime.timedelta(days=x)).strftime("%Y-%m-%d")
        for x in range(0, (end - start).days + offset, increment)
    ]


def optimize_selection_partition(selection: t.Dict, client_name: t.Optional[str] = None) -> t.Dict:
    """Compute right-hand-side values for the selection section of a single partition.

    Used to support custom syntax and optimizations, such as 'all'.
    """
    selection_ = copy.deepcopy(selection)
    # True for direct MARS/ECPublic requests or MARS-brokered CDS datasets.
    is_mars = client_name in ("mars", "ecpublic") or ("class" in selection_ and "stream" in selection_)

    if 'year-month' in selection_.keys():
        ym = selection_['year-month'][0] if isinstance(selection_['year-month'], list) else selection_['year-month']
        year, month = map(int, ym.split('-'))
        last_day = calendar.monthrange(year, month)[1]
        # Expand to explicit YYYY-MM-DD dates supported by both CDS and MARS.
        selection_['date'] = [f'{year:04d}-{month:02d}-{day:02d}' for day in range(1, last_day + 1)]
        del selection_['year-month']

    if "date_range" in selection_.keys():
        # Standard CDS rejects '/to/' syntax; expand to explicit YYYY-MM-DD dates.
        selection_["date"] = (
            _expand_date_range(selection_["date_range"][0])
            if client_name == "cds" and not is_mars
            else selection_["date_range"][0]
        )
        del selection_["date_range"]

    # Convert year/month/day into explicit YYYY-MM-DD dates when day='all' or targeting MARS.
    if "day" in selection_.keys() and (selection_["day"] == "all" or is_mars):
        years, months, days = selection_["year"], selection_["month"], selection_["day"]

        multiples_error = "When using day='all' in selection, '/' is not allowed in {type}."

        if isinstance(years, str):
            years = [years]

        if isinstance(months, str):
            months = [months]

        if isinstance(days, str) and days != "all":
            days = [days]

        date_ranges = []

        # Generating dates for every year-month.
        for year, month in itertools.product(years, months):

            if isinstance(year, str):
                assert "/" not in year, multiples_error.format(type="year")

            if isinstance(month, str):
                assert "/" not in month, multiples_error.format(type="month")

            year, month = int(year), int(month)

            _, n_days_in_month = calendar.monthrange(year, month)

            # Skip invalid calendar days (e.g. Feb 30/31) when explicit days are provided.
            valid_days = (
                range(1, n_days_in_month + 1)
                if days == "all"
                else [int(d) for d in days if 1 <= int(d) <= n_days_in_month]
            )
            date_range = [f'{year:04d}-{month:02d}-{day:02d}' for day in valid_days]

            date_ranges.extend(date_range)

        selection_["date"] = date_ranges
        del selection_["day"]
        del selection_["month"]
        del selection_["year"]

    return selection_
