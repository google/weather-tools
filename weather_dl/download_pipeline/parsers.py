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
"""Parsers for ECMWF download configuration."""

import ast
import configparser
import copy as cp
import datetime
import json
import string
import textwrap
import typing as t
import numpy as np
from collections import OrderedDict
from urllib.parse import urlparse

from .clients import CLIENTS
from .config import Config
from .manifest import MANIFESTS, Manifest, Location, NoOpManifest


def date(candidate: str) -> datetime.date:
    """Converts ECMWF-format date strings into a `datetime.date`.

    Accepted absolute date formats:
    - YYYY-MM-DD
    - YYYYMMDD
    - YYYY-DDD, where DDD refers to the day of the year

    For example:
    - 2021-10-31
    - 19700101
    - 1950-007

    See https://confluence.ecmwf.int/pages/viewpage.action?pageId=118817289 for date format spec.
    Note: Name of month is not supported.
    """
    converted = None

    # Parse relative day value.
    if candidate.startswith('-'):
        return datetime.date.today() + datetime.timedelta(days=int(candidate))

    accepted_formats = ["%Y-%m-%d", "%Y%m%d", "%Y-%j"]

    for fmt in accepted_formats:
        try:
            converted = datetime.datetime.strptime(candidate, fmt).date()
            break
        except ValueError:
            pass

    if converted is None:
        raise ValueError(
            f"Not a valid date: '{candidate}'. Please use valid relative or absolute format."
        )

    return converted


def time(candidate: str) -> datetime.time:
    """Converts ECMWF-format time strings into a `datetime.time`.

    Accepted time formats:
    - HH:MM
    - HHMM
    - HH

    For example:
    - 18:00
    - 1820
    - 18

    Note: If MM is omitted it defaults to 00.
    """
    converted = None

    accepted_formats = ["%H", "%H:%M", "%H%M"]

    for fmt in accepted_formats:
        try:
            converted = datetime.datetime.strptime(candidate, fmt).time()
            break
        except ValueError:
            pass

    if converted is None:
        raise ValueError(
            f"Not a valid time: '{candidate}'. Please use valid format."
        )

    return converted


def day_month_year(candidate: t.Any) -> int:
    """Converts day, month and year strings into 'int'."""
    try:
        if isinstance(candidate, str) or isinstance(candidate, int):
            return int(candidate)
        raise ValueError('must be a str or int.')
    except ValueError as e:
        raise ValueError(
            f"Not a valid day, month, or year value: {candidate}. Please use valid value."
        ) from e


def parse_literal(candidate: t.Any) -> t.Any:
    try:
        # Support parsing ints with leading zeros, e.g. '01'
        if isinstance(candidate, str) and candidate.isdigit():
            return int(candidate)
        return ast.literal_eval(candidate)
    except (ValueError, TypeError, SyntaxError, MemoryError, RecursionError):
        return candidate


def validate(key: str, value: int) -> None:
    """Validates value based on the key."""
    if key == "day":
        assert 1 <= value <= 31, "Day value must be between 1 to 31."
    if key == "month":
        assert 1 <= value <= 12, "Month value must be between 1 to 12."


def typecast(key: str, value: t.Any) -> t.Any:
    """Type the value to its appropriate datatype."""
    SWITCHER = {
        'date': date,
        'time': time,
        'day': day_month_year,
        'month': day_month_year,
        'year': day_month_year,
    }
    converted = SWITCHER.get(key, parse_literal)(value)
    validate(key, converted)
    return converted


def _read_config_file(file: t.IO) -> t.Dict:
    """Reads `*.json` or `*.cfg` files."""
    try:
        return json.load(file)
    except json.JSONDecodeError:
        pass

    file.seek(0)

    try:
        config = configparser.ConfigParser()
        config.read_file(file)
        config = {s: dict(config.items(s)) for s in config.sections()}
        return config
    except configparser.ParsingError:
        return {}


def parse_config(file: t.IO) -> t.Dict:
    """Parses a `*.json` or `*.cfg` file into a configuration dictionary."""
    config = _read_config_file(file)
    config_by_section = {s: _parse_lists(v, s) for s, v in config.items()}
    config_with_nesting = parse_subsections(config_by_section)
    return config_with_nesting


def parse_manifest(location: Location, pipeline_opts: t.Dict) -> Manifest:
    """Constructs a manifest object by parsing the location."""
    project_id__exists = 'project' in pipeline_opts
    project_id__not_set = 'projectId' not in location

    # If the firestore location doesn't specify which project (and, the pipeline
    # knows which project)...
    if location.startswith('fs://') and project_id__not_set and project_id__exists:
        # ...Set the project query param in the Firestore URI.
        start_char = '&' if '?' in location else '?'
        project = pipeline_opts.get('project')
        location += f'{start_char}projectId={project}'

    parsed = urlparse(location)
    return MANIFESTS.get(parsed.scheme, NoOpManifest)(location)


def _splitlines(block: str) -> t.List[str]:
    """Converts a multi-line block into a list of strings."""
    return [line.strip() for line in block.strip().splitlines()]


def mars_range_value(token: str) -> t.Union[datetime.date, int, float]:
    """Converts a range token into either a date, int, or float."""
    # TODO(b/175432034): Recognize time values
    try:
        return date(token)
    except ValueError:
        pass

    if token.isdecimal():
        return int(token)

    try:
        return float(token)
    except ValueError:
        raise ValueError("Token string must be an 'int', 'float', or 'datetime.date()'.")


def mars_increment_value(token: str) -> t.Union[int, float]:
    """Converts an increment token into either an int or a float."""
    try:
        return int(token)
    except ValueError:
        pass

    try:
        return float(token)
    except ValueError:
        raise ValueError("Token string must be an 'int' or a 'float'.")


def parse_mars_syntax(block: str) -> t.List[str]:
    """Parses MARS list or range into a list of arguments; ranges are inclusive.

    Types for the range and value are inferred.

    Examples:
        >>> parse_mars_syntax("10/to/12")
        ['10', '11', '12']
        >>> parse_mars_syntax("12/to/10/by/-1")
        ['12', '11', '10']
        >>> parse_mars_syntax("0.0/to/0.5/by/0.1")
        ['0.0', '0.1', '0.2', '0.30000000000000004', '0.4', '0.5']
        >>> parse_mars_syntax("2020-01-07/to/2020-01-14/by/2")
        ['2020-01-07', '2020-01-09', '2020-01-11', '2020-01-13']
        >>> parse_mars_syntax("2020-01-14/to/2020-01-07/by/-2")
        ['2020-01-14', '2020-01-12', '2020-01-10', '2020-01-08']

    Returns:
        A list of strings representing a range from start to finish, based on the
        type of the values in the range.
        If all range values are integers, it will return a list of strings of integers.
        If range values are floats, it will return a list of strings of floats.
        If the range values are dates, it will return a list of strings of dates in
        YYYY-MM-DD format. (Note: here, the increment value should be an integer).
    """

    # Split into tokens, omitting empty strings.
    tokens = [b.strip() for b in block.split('/') if b != '']

    # Return list if no range operators are present.
    if 'to' not in tokens and 'by' not in tokens:
        return tokens

    # Parse range values, honoring 'to' and 'by' operators.
    try:
        to_idx = tokens.index('to')
        assert to_idx != 0, "There must be a start token."
        start_token, end_token = tokens[to_idx - 1], tokens[to_idx + 1]
        start, end = mars_range_value(start_token), mars_range_value(end_token)

        # Parse increment token, or choose default increment.
        increment_token = '1'
        increment = 1
        if 'by' in tokens:
            increment_token = tokens[tokens.index('by') + 1]
            increment = mars_increment_value(increment_token)
    except (AssertionError, IndexError, ValueError):
        raise SyntaxError(f"Improper range syntax in '{block}'.")

    # Return a range of values with appropriate data type.
    if isinstance(start, datetime.date) and isinstance(end, datetime.date):
        if not isinstance(increment, int):
            raise ValueError(
                f"Increments on a date range must be integer number of days, '{increment_token}' is invalid."
            )
        return [d.strftime("%Y-%m-%d") for d in date_range(start, end, increment)]
    elif (isinstance(start, float) or isinstance(end, float)) and not isinstance(increment, datetime.date):
        # Increment can be either an int or a float.
        _round_places = 4
        return [str(round(x, _round_places)).zfill(len(start_token))
                for x in np.arange(start, end + increment, increment)]
    elif isinstance(start, int) and isinstance(end, int) and isinstance(increment, int):
        # Honor leading zeros.
        offset = 1 if start <= end else -1
        return [str(x).zfill(len(start_token)) for x in range(start, end + offset, increment)]
    else:
        raise ValueError(
            f"Range tokens (start='{start_token}', end='{end_token}', increment='{increment_token}')"
            f" are inconsistent types."
        )


def date_range(start: datetime.date, end: datetime.date, increment: int = 1) -> t.Iterable[datetime.date]:
    """Gets a range of dates, inclusive."""
    offset = 1 if start <= end else -1
    return (start + datetime.timedelta(days=x) for x in range(0, (end - start).days + offset, increment))


def _parse_lists(config: dict, section: str = '') -> t.Dict:
    """Parses multiline blocks in *.cfg and *.json files as lists."""
    for key, val in config.items():
        # Checks str type for backward compatibility since it also support "padding": 0 in json config
        if not isinstance(val, str):
            continue

        if '/' in val and 'parameters' not in section:
            config[key] = parse_mars_syntax(val)
        elif '\n' in val:
            config[key] = _splitlines(val)

    return config


def _number_of_replacements(s: t.Text):
    format_names = [v[1] for v in string.Formatter().parse(s) if v[1] is not None]
    num_empty_names = len([empty for empty in format_names if empty == ''])
    if num_empty_names != 0:
        num_empty_names -= 1
    return len(set(format_names)) + num_empty_names


def parse_subsections(config: t.Dict) -> t.Dict:
    """Interprets [section.subsection] as nested dictionaries in `.cfg` files."""
    copy = cp.deepcopy(config)
    for key, val in copy.items():
        path = key.split('.')
        runner = copy
        parent = {}
        p = None
        for p in path:
            if p not in runner:
                runner[p] = {}
            parent = runner
            runner = runner[p]
        parent[p] = val

    for_cleanup = [key for key, _ in copy.items() if '.' in key]
    for target in for_cleanup:
        del copy[target]
    return copy


def process_config(file: t.IO) -> Config:
    """Read the config file and prompt the user if it is improperly structured."""
    config = parse_config(file)

    def require(condition: bool, message: str, error_type: t.Type[Exception] = ValueError) -> None:
        """A assert-like helper that wraps text and throws an error."""
        if not condition:
            raise error_type(textwrap.dedent(message))

    require(bool(config), "Unable to parse configuration file.")
    require('parameters' in config,
            """
            'parameters' section required in configuration file.

            The 'parameters' section specifies the 'client', 'dataset', 'target_path', and
            'partition_key' for the API client.

            Please consult the documentation for more information.""")

    params = config.get('parameters', {})
    require('target_template' not in params,
            """
            'target_template' is deprecated, use 'target_path' instead.

            Please consult the documentation for more information.""")
    require('target_path' in params,
            """
            'parameters' section requires a 'target_path' key.

            The 'target_path' is used to format the name of the output files. It
            accepts Python 3.5+ string format symbols (e.g. '{}'). The number of symbols
            should match the length of the 'partition_keys', as the 'partition_keys' args
            are used to create the templates.""")
    require('client' in params,
            """
            'parameters' section requires a 'client' key.

            Supported clients are {}
            """.format(str(list(CLIENTS.keys()))))
    require(params.get('client') in CLIENTS.keys(),
            """
            Invalid 'client' parameter.

            Supported clients are {}
            """.format(str(list(CLIENTS.keys()))))
    require('append_date_dirs' not in params,
            """
            The current version of 'google-weather-tools' no longer supports 'append_date_dirs'!

            Please refer to documentation for creating date-based directory hierarchy :
            https://weather-tools.readthedocs.io/en/latest/Configuration.html#"""
            """creating-a-date-based-directory-hierarchy.""",
            NotImplementedError)
    require('target_filename' not in params,
            """
            The current version of 'google-weather-tools' no longer supports 'target_filename'!

            Please refer to documentation :
            https://weather-tools.readthedocs.io/en/latest/Configuration.html#parameters-section.""",
            NotImplementedError)

    partition_keys = params.get('partition_keys', list())
    if isinstance(partition_keys, str):
        partition_keys = [partition_keys.strip()]

    selection = config.get('selection', dict())
    require(all((key in selection for key in partition_keys)),
            """
            All 'partition_keys' must appear in the 'selection' section.

            'partition_keys' specify how to split data for workers. Please consult
            documentation for more information.""")

    num_template_replacements = _number_of_replacements(params['target_path'])
    num_partition_keys = len(partition_keys)

    require(num_template_replacements == num_partition_keys,
            """
            'target_path' has {0} replacements. Expected {1}, since there are {1}
            partition keys.
            """.format(num_template_replacements, num_partition_keys))

    if 'day' in partition_keys:
        require(selection['day'] != 'all',
                """If 'all' is used for a selection value, it cannot appear as a partition key.""")

    # Ensure consistent lookup.
    config['parameters']['partition_keys'] = partition_keys

    # Ensure the cartesian-cross can be taken on singleton values for the partition.
    for key in partition_keys:
        if not isinstance(selection[key], list):
            selection[key] = [selection[key]]

    return Config.from_dict(config)


def prepare_target_name(config: Config) -> str:
    """Returns name of target location."""
    partition_dict = OrderedDict((key, typecast(key, config.selection[key][0])) for key in config.partition_keys)
    target = config.target_path.format(*partition_dict.values(), **partition_dict)

    return target


def get_subsections(config: Config) -> t.List[t.Tuple[str, t.Dict]]:
    """Collect parameter subsections from main configuration.

    If the `parameters` section contains subsections (e.g. '[parameters.1]',
    '[parameters.2]'), collect the subsection key-value pairs. Otherwise,
    return an empty dictionary (i.e. there are no subsections).

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
    return [(name, params) for name, params in config.kwargs.items()
            if isinstance(params, dict)] or [('default', {})]
