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


import abc
import logging
import dataclasses
import typing as t
import json
from time import time
from itertools import cycle
from shutil import get_terminal_size
from threading import Thread
from time import sleep
from tabulate import tabulate

logger = logging.getLogger(__name__)


def timeit(func):
    def wrap_func(*args, **kwargs):
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
        print(f"[executed in {(t2-t1):.4f}s.]")
        return result

    return wrap_func


def as_table(response: str):
    data = json.loads(response)

    if not isinstance(data, list):
        # convert response to list if not a list.
        data = [data]

    if len(data) == 0:
        return ""

    header = data[0].keys()
    # if any column has lists, convert that to a string.
    rows = [
        [
            ",\n".join([f"{i} {ele}" for i, ele in enumerate(val)])
            if isinstance(val, list)
            else val
            for val in x.values()
        ]
        for x in data
    ]
    rows.insert(0, list(header))
    return tabulate(
        rows, showindex=True, tablefmt="grid", maxcolwidths=[16] * len(header)
    )


class Loader:

    def __init__(self, desc="Loading...", end="", timeout=0.1):
        """
        A loader-like context manager

        Args:
            desc (str, optional): The loader's description. Defaults to "Loading...".
            end (str, optional): Final print. Defaults to "Done!".
            timeout (float, optional): Sleep time between prints. Defaults to 0.1.
        """
        self.desc = desc
        self.end = end
        self.timeout = timeout

        self._thread = Thread(target=self._animate, daemon=True)
        self.steps = ["⢿", "⣻", "⣽", "⣾", "⣷", "⣯", "⣟", "⡿"]
        self.done = False

    def start(self):
        self._thread.start()
        return self

    def _animate(self):
        for c in cycle(self.steps):
            if self.done:
                break
            print(f"\r{self.desc} {c}", flush=True, end="")
            sleep(self.timeout)

    def __enter__(self):
        self.start()

    def stop(self):
        self.done = True
        cols = get_terminal_size((80, 20)).columns
        print("\r" + " " * cols, end="", flush=True)

    def __exit__(self, exc_type, exc_value, tb):
        # handle exceptions with those variables ^
        self.stop()


@dataclasses.dataclass
class Validator(abc.ABC):
    valid_keys: t.List[str]

    def validate(
        self, filters: t.List[str], show_valid_filters=True, allow_missing: bool = False
    ):
        """Validate keys of filter list."""
        filter_dict = {}

        for filter in filters:
            _filter = filter.split("=")

            if len(_filter) != 2:
                if show_valid_filters:
                    logger.info(f"valid filters are: {self.valid_keys}.")
                raise ValueError("Incorrect Filter. Please Try again.")

            key, value = _filter
            filter_dict[key] = value

        data_set = set(filter_dict.keys())
        valid_set = set(self.valid_keys)

        if self._validate_keys(data_set, valid_set, allow_missing):
            return filter_dict

    def validate_json(self, file_path, allow_missing: bool = False):
        """Validate keys in a json file."""
        try:
            with open(file_path) as f:
                data: dict = json.load(f)
                data_keys = data.keys()

                data_set = set(data_keys)
                valid_set = set(self.valid_keys)

                if self._validate_keys(data_set, valid_set, allow_missing):
                    return data

        except FileNotFoundError:
            logger.info("file not found.")
            raise FileNotFoundError

    def _validate_keys(self, data_set: set, valid_set: set, allow_missing: bool):
        """Function that validates if keys match the valid set."""
        missing_keys = valid_set.difference(data_set)
        invalid_keys = data_set.difference(valid_set)

        if not allow_missing and len(missing_keys) > 0:
            raise ValueError(f"keys {missing_keys} are missing in file.")

        if len(invalid_keys) > 0:
            raise ValueError(f"keys {invalid_keys} are invalid keys.")

        if allow_missing or data_set == valid_set:
            return True

        return False
