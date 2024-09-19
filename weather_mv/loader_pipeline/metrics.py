# Copyright 2024 Google LLC
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

"""Utilities for adding metrics to beam pipeline."""

import copy
import datetime
import inspect
import logging
import time
import typing as t
from collections import OrderedDict
from functools import wraps

import apache_beam as beam
from apache_beam.metrics import metric

from .sinks import get_file_time

logger = logging.getLogger(__name__)


def timeit(func_name: str, keyed_fn: bool = False):
    """Decorator to add time it takes for an element to be processed by a stage.

    Args:
        func_name: A unique name of the stage.
        keyed_fn (optional): This has to be passed true if the input is adding keys to the element.

        For example a stage like

        class Shard(beam.DoFn):
            @timeit('Sharding', keyed_fn=True)
            def process(self,element):
                key = randrange(10)
                yield key, element

        We are passing `keyed_fn=True` as we are adding a key to our element. Usually keys are added
        to later group the element by a `GroupBy` stage.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # If metrics are turned off, don't do anything.
            if not hasattr(self, "use_metrics") or (
                hasattr(self, "use_metrics") and not self.use_metrics
            ):
                for result in func(self, *args, **kwargs):
                    yield result

                return

            time_dict = OrderedDict()

            # Only the first timer wrapper will have no time_dict.
            # All subsequent wrappers can extract out the dict.
            # args 0 would be a tuple.
            if len(args[0]) == 1:
                raise ValueError("time_dict not found.")

            element, time_dict = args[0]
            args = (element,) + args[1:]

            if not isinstance(time_dict, OrderedDict):
                raise ValueError("time_dict not found.")

            # If the function is a generator, yield the output
            # othewise return it.
            if inspect.isgeneratorfunction(func):
                for result in func(self, *args, **kwargs):
                    new_time_dict = copy.deepcopy(time_dict)
                    if func_name in new_time_dict:
                        del new_time_dict[func_name]
                    new_time_dict[func_name] = time.time()
                    if keyed_fn:
                        (key, element) = result
                        yield key, (element, new_time_dict)
                    else:
                        yield result, new_time_dict
            else:
                raise ValueError("Function is not a generator.")

        return wrapper

    return decorator


class AddTimer(beam.DoFn):
    """DoFn to add a empty time_dict per element in PCollection. This dict will
    stage_names as keys and the time it took for that element in that stage."""

    def process(self, element) -> t.Iterator[t.Any]:
        time_dict = OrderedDict([
            ("uri", element),
            ("bucket", get_file_time(element)),
            ("pickup", time.time()),
        ])
        yield element, time_dict


class AddMetrics(beam.DoFn):
    """DoFn to add Element Processing Time metric to beam. Expects PCollection
    to contain a time_dict."""

    def __init__(self, asset_start_time_format: str = "%Y-%m-%dT%H:%M:%SZ"):
        super().__init__()
        self.element_processing_time = metric.Metrics.distribution(
            "Time", "element_processing_time_ms"
        )
        self.data_latency_time = metric.Metrics.distribution(
            "Time", "data_latency_time_ms"
        )
        self.asset_start_time_format = asset_start_time_format

    def process(self, element):
        try:
            if len(element) == 0:
                raise ValueError("time_dict not found.")
            (_, asset_start_time), time_dict = element
            if not isinstance(time_dict, OrderedDict):
                raise ValueError("time_dict not found.")

            uri = time_dict.pop("uri", _)

            time_dict_values = list(time_dict.values())
            element_processing_time = (time_dict_values[-1] - time_dict_values[0])
            self.element_processing_time.update(int(element_processing_time))

            # Adding data latency.
            if asset_start_time:
                current_time = time.time()
                asset_start_time = datetime.datetime.strptime(
                    asset_start_time, self.asset_start_time_format
                ).timestamp()

                # Converting seconds to milli seconds.
                data_latency_ms = (current_time - asset_start_time) * 1000
                self.data_latency_time.update(int(data_latency_ms))

                # Logging file init to bucket time as well.
                time_dict.update({"FileInit": asset_start_time})
                time_dict.move_to_end('FileInit', last=False)

            # Logging time taken by each step...
            time_dict_keys = list(time_dict.keys())
            time_dict_values = list(time_dict.values())
            for i in range(len(time_dict) - 1):
                step_time = round(time_dict_values[i+1] - time_dict_values[i])
                logger.info(
                    f"{uri}: Time from {time_dict_keys[i]} -> "
                    f"{time_dict_keys[i+1]}: {step_time} seconds."
                )

        except Exception as e:
            logger.warning(
                f"Some error occured while adding metrics. Error {e}"
            )
