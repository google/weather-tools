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

import configparser
import copy as cp
import datetime
import io
import json
import string
import textwrap
import typing as t
from urllib.parse import urlparse

from .clients import CLIENTS
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


def parse_config(file: io.StringIO) -> t.Dict:
    """Parses a `*.json` or `*.cfg` file into a configuration dictionary."""
    try:
        # TODO(b/175429166): JSON files do not support MARs range syntax.
        return json.load(file)
    except json.JSONDecodeError:
        pass

    file.seek(0)

    try:
        config = configparser.ConfigParser()
        config.read_file(file)
        config_by_section = {s: _parse_lists(config, s) for s in config.sections()}
        config_with_nesting = parse_subsections(config_by_section)
        return config_with_nesting
    except configparser.ParsingError:
        pass

    return {}


def parse_manifest_location(location: Location, pipeline_opts: t.Dict) -> Manifest:
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


def parse_mars_syntax(block: str) -> t.List[str]:
    """Parses MARS list or range into a list of arguments; ranges are inclusive.

    Types for the range and value are inferred.

    Examples:
        >>> parse_mars_syntax("10/to/12")
        ['10', '11', '12']
        >>> parse_mars_syntax("0.0/to/0.5/by/0.1")
        ['0.0', '0.1', '0.2', '0.30000000000000004', '0.4', '0.5']
        >>> parse_mars_syntax("2020-01-07/to/2020-01-14/by/2")
        ['2020-01-07', '2020-01-09', '2020-01-11', '2020-01-13']

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
            increment = mars_range_value(increment_token)
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
        out = []
        x = start
        while x <= end:
            out.append(str(x))
            x += increment
        return out
    elif isinstance(start, int) and isinstance(end, int) and isinstance(increment, int):
        # Honor leading zeros.
        return [str(x).zfill(len(start_token)) for x in range(start, end + 1, increment)]
    else:
        raise ValueError(
            f"Range tokens (start='{start_token}', end='{end_token}', increment='{increment_token}')"
            f" are inconsistent types."
        )


def date_range(start: datetime.date, end: datetime.date, increment: int = 1) -> t.Iterable[datetime.date]:
    """Gets a range of dates, inclusive."""
    return (start + datetime.timedelta(days=x) for x in range(0, (end - start).days + 1, increment))


def _parse_lists(config_parser: configparser.ConfigParser, section: str = '') -> t.Dict:
    """Parses multiline blocks in *.cfg files as lists."""
    config = dict(config_parser.items(section))

    for key, val in config.items():
        if '/' in val and 'parameters' not in section:
            config[key] = parse_mars_syntax(val)
        elif '\n' in val:
            config[key] = _splitlines(val)

    return config


def _number_of_replacements(s: t.Text):
    return len([v for v in string.Formatter().parse(s) if v[1] is not None])


def use_date_as_directory(config: t.Dict):
    return 'date' in config['parameters']['partition_keys'] \
           and config['parameters'].get('append_date_dirs', 'false') == 'true' \
           and 'target_filename' in config['parameters']


def parse_subsections(config: t.Dict) -> t.Dict:
    """Interprets [section.subsection] as nested dictionaries in `.cfg` files.

    Also counts number of 'api_key' fields found.
    """
    copy = cp.deepcopy(config)
    num_api_keys = 0
    for key, val in copy.items():
        path = key.split('.')
        if val.get('api_key', ''):
            num_api_keys += 1
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
    if num_api_keys:
        copy['parameters']['num_api_keys'] = num_api_keys
    return copy


def process_config(file: io.StringIO) -> t.Dict:
    """Read the config file and prompt the user if it is improperly structured."""
    config = parse_config(file)

    def require(condition: bool, message: str) -> None:
        """A assert-like helper that wraps text and throws a `ValueError`."""
        if not condition:
            raise ValueError(textwrap.dedent(message))

    require(bool(config), "Unable to parse configuration file.")
    require('parameters' in config,
            """
            'parameters' section required in configuration file.

            The 'parameters' section specifies the 'dataset', 'target_path', and
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
    if params.get('append_date_dirs', 'false') == 'true':
        require(use_date_as_directory(config),
                """
                'append_date_dirs' set to true, but creating the date directory hierarchy also
                requires that 'target_filename' is given and that 'date' is a partition_key.
                """)

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
    if 'target_filename' in params:
        num_template_replacements += _number_of_replacements(params['target_filename'])
    num_partition_keys = len(partition_keys)
    if use_date_as_directory(config):
        num_partition_keys -= 1
        if str(params.get('target_path')).endswith('/'):
            params['target_path'] = params.get('target_path')[:-1]

    require(num_template_replacements == num_partition_keys,
            """
            'target_path' has {0} replacements. Expected {1}, since there are {1}
            partition keys.

            Note: If date is used to create a directory hierarchy
            no replacement is needed for 'date')
            """.format(num_template_replacements, num_partition_keys))

    # Ensure consistent lookup.
    config['parameters']['partition_keys'] = partition_keys

    return config
