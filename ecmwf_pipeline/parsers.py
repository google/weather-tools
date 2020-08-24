import argparse
import configparser
import datetime
import io
import json
import typing as t
import textwrap


def date(candidate: str) -> datetime.date:
    """Converts 'YYYY-MM-DD' formatted string into a `datetime.date`."""
    try:
        return datetime.datetime.strptime(candidate, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(
            "Not a valid date: '{}'. Please use 'YYYY-MM-DD' format.'".format(candidate)
        )


def parse_config(file: io.StringIO) -> t.Dict:
    """Parses a *.json or *.cfg file ito a configuration dictionary."""
    try:
        return json.load(file)
    except json.JSONDecodeError:
        pass

    file.seek(0)

    try:
        config = configparser.ConfigParser()
        config.read_file(file)
        return {s: _parse_lists(dict(config.items(s))) for s in config.sections()}
    except configparser.ParsingError:
        pass

    return {}


def _splitlines(block: str) -> t.List[str]:
    """Converts a multi-line block into a list of strings."""
    return [line.strip() for line in block.strip().splitlines()]


def _parse_lists(config: t.Dict) -> t.Dict:
    """Parses multiline blocks in *.cfg files as lists."""
    for key, val in config.items():
        if '\n' in val:
            config[key] = _splitlines(val)
    return config


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
            
            The 'parameters' section specifies the 'dataset', 'target_template', and
            'partition_key' for the API client.
            
            Please consult the documentation for more information.""")

    params = config.get('parameters', {})
    require('dataset' in params,
            """
            'parameters' section requires a 'dataset' key.
            
            The 'dataset' value is used to choose which data product to download from the
            API client.
            
            Please consult the client documentation for more information about what value 
            to specify.""")
    require('target_template' in params,
            """
            'parameters' section requires a 'target_template' key.
            
            The 'target_template' is used to format the name of the output files. It
            accepts Python 3.5+ string format symbols (e.g. '{}'). The number of symbols
            should match the length of the 'partition_keys', as the 'partition_keys' args
            are used to create the templates.""")

    partition_keys = params.get('partition_keys', list())
    if isinstance(partition_keys, str):
        partition_keys = [partition_keys.strip()]

    selection = config.get('selection', dict())
    require(all((key in selection for key in partition_keys)),
            """
            All 'partition_keys' must appear in the 'selection' section.
            
            'partition_keys' specify how to split data for workers. Please consult 
            documentation for more information.""")

    # Ensure consistent lookup.
    config['parameters']['partition_keys'] = partition_keys

    return config
