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

import time
import copy
import inspect
from functools import wraps
import apache_beam as beam
from apache_beam.metrics import metric


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
            start_time = time.time()
            time_dict = {}

            # Only the first timer wrapper will have no time_dict.
            # All subsequent wrappers can extract out the dict.
            # args 0 would be a tuple.
            if len(args[0]) == 1:
                raise ValueError('time_dict not found.')

            element, time_dict = args[0]
            args = (element,) + args[1:]

            if not isinstance(time_dict, dict):
                raise ValueError('time_dict not found.')

            # If the function is a generator, yield the output
            # othewise return it.
            if inspect.isgeneratorfunction(func):
                for result in func(self, *args, **kwargs):
                    end_time = time.time()
                    processing_time = end_time - start_time
                    new_time_dict = copy.deepcopy(time_dict)
                    new_time_dict[func_name] = processing_time
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
    """DoFn to add a empty time_dict per element in PCollection. This dict will stage_names as keys
    and the time it took for that element in that stage."""
    def process(self, element):
        time_dict = {}
        yield element, time_dict


class AddMetrics(beam.DoFn):
    """DoFn to add Element Processing Time metric to beam. Expects PCollection to contain a time_dict."""

    def __init__(self):
        super().__init__()
        self.element_processing_time = metric.Metrics.distribution('Time', 'element_processing_time_ms')

    def process(self, element):
        if len(element) == 0:
            raise ValueError("time_dict not found.")
        _, time_dict = element
        if not isinstance(time_dict, dict):
            raise ValueError("time_dict not found.")

        total_time = 0
        for stage_time in time_dict.values():
            total_time += stage_time

        self.element_processing_time.update(int(total_time * 1000))
