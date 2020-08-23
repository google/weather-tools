import argparse
import configparser
import datetime
import io
import json
import typing as t


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
        return {s: dict(config.items(s)) for s in config.sections()}
    except configparser.ParsingError:
        pass

    return {}
