import logging
import datetime
import geojson
import hashlib
import itertools
import os
import socket
import subprocess
import sys
import typing as t

import numpy as np
import pandas as pd
from apache_beam.io.gcp import gcsio
from apache_beam.utils import retry
from xarray.core.utils import ensure_us_time_resolution
from urllib.parse import urlparse
from google.api_core.exceptions import BadRequest


LATITUDE_RANGE = (-90, 90)
LONGITUDE_RANGE = (-180, 180)
GLOBAL_COVERAGE_AREA = [90, -180, -90, 180]

logger = logging.getLogger(__name__)


def _retry_if_valid_input_but_server_or_socket_error_and_timeout_filter(
    exception,
) -> bool:
    if isinstance(exception, socket.timeout):
        return True
    if isinstance(exception, TimeoutError):
        return True
    # To handle the concurrency issue in BigQuery.
    if isinstance(exception, BadRequest):
        return True
    return retry.retry_if_valid_input_but_server_error_and_timeout_filter(exception)


class _FakeClock:

    def sleep(self, value):
        pass


def retry_with_exponential_backoff(fun):
    """A retry decorator that doesn't apply during test time."""
    clock = retry.Clock()

    # Use a fake clock only during test time...
    if "unittest" in sys.modules.keys():
        clock = _FakeClock()

    return retry.with_exponential_backoff(
        retry_filter=_retry_if_valid_input_but_server_or_socket_error_and_timeout_filter,
        clock=clock,
    )(fun)


# TODO(#245): Group with common utilities (duplicated)
def ichunked(iterable: t.Iterable, n: int) -> t.Iterator[t.Iterable]:
    """Yield evenly-sized chunks from an iterable."""
    input_ = iter(iterable)
    try:
        while True:
            it = itertools.islice(input_, n)
            # peek to check if 'it' has next item.
            first = next(it)
            yield itertools.chain([first], it)
    except StopIteration:
        pass


# TODO(#245): Group with common utilities (duplicated)
def copy(src: str, dst: str) -> None:
    """Copy data via `gsutil cp`."""
    try:
        subprocess.run(["gsutil", "cp", src, dst], check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        logger.info(
            f'Failed to copy file {src!r} to {dst!r} due to {e.stderr.decode("utf-8")}.'
        )
        raise


# TODO(#245): Group with common utilities (duplicated)
def to_json_serializable_type(value: t.Any) -> t.Any:
    """Returns the value with a type serializable to JSON"""
    # Note: The order of processing is significant.
    logger.info("Serializing to JSON.")

    if pd.isna(value) or value is None:
        return None
    elif np.issubdtype(type(value), np.floating):
        return float(value)
    elif type(value) == np.ndarray:
        # Will return a scaler if array is of size 1, else will return a list.
        return value.tolist()
    elif (
        type(value) == datetime.datetime
        or type(value) == str
        or type(value) == np.datetime64
    ):
        # Assume strings are ISO format timestamps...
        try:
            value = datetime.datetime.fromisoformat(value)
        except ValueError:
            # ... if they are not, assume serialization is already correct.
            return value
        except TypeError:
            # ... maybe value is a numpy datetime ...
            try:
                value = ensure_us_time_resolution(value).astype(datetime.datetime)
            except AttributeError:
                # ... value is a datetime object, continue.
                pass

        # We use a string timestamp representation.
        if value.tzname():
            return value.isoformat()

        # We assume here that naive timestamps are in UTC timezone.
        return value.replace(tzinfo=datetime.timezone.utc).isoformat()
    elif type(value) == np.timedelta64:
        # Return time delta in seconds.
        return float(value / np.timedelta64(1, "s"))
    # This check must happen after processing np.timedelta64 and np.datetime64.
    elif np.issubdtype(type(value), np.integer):
        return int(value)

    return value


def fetch_geo_polygon(area: t.Union[list, str]) -> str:
    """Calculates a geography polygon from an input area."""
    # Ref: https://confluence.ecmwf.int/pages/viewpage.action?pageId=151520973
    if isinstance(area, str):
        # European area
        if area == "E":
            area = [73.5, -27, 33, 45]
        # Global area
        elif area == "G":
            area = GLOBAL_COVERAGE_AREA
        else:
            raise RuntimeError(f"Not a valid value for area in config: {area}.")

    n, w, s, e = [float(x) for x in area]
    if s < LATITUDE_RANGE[0]:
        raise ValueError(f"Invalid latitude value for south: '{s}'")
    if n > LATITUDE_RANGE[1]:
        raise ValueError(f"Invalid latitude value for north: '{n}'")
    if w < LONGITUDE_RANGE[0]:
        raise ValueError(f"Invalid longitude value for west: '{w}'")
    if e > LONGITUDE_RANGE[1]:
        raise ValueError(f"Invalid longitude value for east: '{e}'")

    # Define the coordinates of the bounding box.
    coords = [[w, n], [w, s], [e, s], [e, n], [w, n]]

    # Create the GeoJSON polygon object.
    polygon = geojson.dumps(geojson.Polygon([coords]))
    return polygon


def get_file_size(path: str) -> float:
    parsed_gcs_path = urlparse(path)
    if parsed_gcs_path.scheme != "gs" or parsed_gcs_path.netloc == "":
        return os.stat(path).st_size / (1024**3) if os.path.exists(path) else 0
    else:
        return (
            gcsio.GcsIO().size(path) / (1024**3) if gcsio.GcsIO().exists(path) else 0
        )


def get_wait_interval(num_retries: int = 0) -> float:
    """Returns next wait interval in seconds, using an exponential backoff algorithm."""
    if 0 == num_retries:
        return 0
    return 2**num_retries


def generate_md5_hash(input: str) -> str:
    """Generates md5 hash for the input string."""
    return hashlib.md5(input.encode("utf-8")).hexdigest()


def download_with_aria2(url: str, path: str) -> None:
    """Downloads a file from the given URL using the `aria2c` command-line utility,
    with options set to improve download speed and reliability."""
    dir_path, file_name = os.path.split(path)
    try:
        subprocess.run(
            [
                "aria2c",
                "-x",
                "16",
                "-s",
                "16",
                url,
                "-d",
                dir_path,
                "-o",
                file_name,
                "--allow-overwrite",
            ],
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError as e:
        logger.info(
            f'Failed download from server {url!r} to {path!r} due to {e.stderr.decode("utf-8")}.'
        )
        raise
