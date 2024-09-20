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
import dataclasses
import datetime
import inspect
import logging
import time
import typing as t
from collections import OrderedDict

import apache_beam as beam
from apache_beam.metrics import metric
from apache_beam.transforms import window, trigger
from functools import wraps
from google.cloud import monitoring_v3

from .sinks import get_file_time, KwargsFactoryMixin

logger = logging.getLogger(__name__)

# For Metrics API retry logic.
INITIAL_DELAY = 1.0  # Initial delay in seconds.
MAX_DELAY = 600  # Maximum delay before giving up in seconds.
NUM_RETRIES = 10  # Number of tries with exponential backoff.
TASK_QUEUE_WAIT_TIME = 120  # Task queue wait time in seconds.


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


@dataclasses.dataclass
class AddTimer(beam.DoFn, KwargsFactoryMixin):
    """DoFn to add a time_dict with uri, file time in GCS bucket, when it was
    picked up in PCollection. This dict will contain each stage_names as keys
    and the timestamp when it finished that step's execution."""

    topic: t.Optional[str] = None

    def process(self, element) -> t.Iterator[t.Any]:
        time_dict = OrderedDict(
            [
                ("uri", element),
                ("bucket", get_file_time(element) if self.topic else time.time()),
                ("pickup", time.time()),
            ]
        )
        yield element, time_dict


class AddBeamMetrics(beam.DoFn):
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

            # Time for a file to get ingested into EE from when it appeared in bucket.
            # When the pipeline is in batch mode, it will be from when the file
            # was picked up by the pipeline.
            element_processing_time = (
                time_dict["IngestIntoEE"] - time_dict["bucket"]
            ) * 1000
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
                time_dict.move_to_end("FileInit", last=False)

            # Logging time taken by each step...
            for (current_step, current_time), (next_step, next_time) in zip(
                time_dict.items(), list(time_dict.items())[1:]
            ):
                step_time = round(next_time - current_time)
                logger.info(
                    f"{uri}: Time from {current_step} -> {next_step}: {step_time} seconds."
                )
            yield ("custom_metrics", (data_latency_ms / 1000, element_processing_time / 1000))
        except Exception as e:
            logger.warning(
                f"Some error occured while adding metrics. Error {e}"
            )


@dataclasses.dataclass
class CreateTimeSeries(beam.DoFn):
    """DoFn to write metrics TimeSeries data in Google Cloud Monitoring."""
    job_name: str
    project: str
    region: str

    def create_time_series(self, metric_name: str, metric_value: float) -> None:
        """Writes data to a Metrics TimeSeries."""
        client = monitoring_v3.MetricServiceClient()
        series = monitoring_v3.TimeSeries()
        series.metric.type = f"custom.googleapis.com/{metric_name}"
        series.metric.labels["description"] = metric_name
        series.resource.type = "dataflow_job"
        series.resource.labels["job_name"] = self.job_name
        series.resource.labels["project_id"] = self.project
        series.resource.labels["region"] = self.region

        now = time.time()
        seconds = int(now)
        nanos = int((now - seconds) * 10**9)
        interval = monitoring_v3.TimeInterval(
            {"end_time": {"seconds": seconds, "nanos": nanos}}
        )

        point = monitoring_v3.Point(
            {"interval": interval, "value": {"double_value": metric_value}}
        )
        series.points = [point]
        client.create_time_series(
            name=f"projects/{self.project}", time_series=[series]
        )
        logger.info(
            f"Successfully created time series for {metric_name}. Metric value: {metric_value}."
        )

    def process(self, element: t.Any):
        _, metric_values = element
        data_latency_times = [x[0] for x in metric_values]
        element_processing_times = [x[1] for x in metric_values]

        logger.info(f"data_latency_time values: {data_latency_times}")
        self.create_time_series("data_latency_time_max", max(data_latency_times))
        self.create_time_series(
            "data_latency_time_mean", sum(data_latency_times) / len(data_latency_times)
        )

        logger.info(f"element_processing_time values: {element_processing_times}")
        self.create_time_series("element_processing_time_max", max(element_processing_times))
        self.create_time_series(
            "element_processing_time_mean", sum(element_processing_times) / len(element_processing_times)
        )


@dataclasses.dataclass
class AddMetrics(beam.PTransform, KwargsFactoryMixin):
    job_name: str
    project: str
    region: str

    def expand(self, pcoll: beam.PCollection):
        return (
            pcoll
            | "AddBeamMetrics" >> beam.ParDo(AddBeamMetrics())
            | "AddTimestamps"
            >> beam.Map(
                lambda element: window.TimestampedValue(element, time.time())
            )
            | "Window"
            >> beam.WindowInto(
                window.GlobalWindows(),
                trigger=trigger.Repeatedly(trigger.AfterProcessingTime(5)),
                accumulation_mode=trigger.AccumulationMode.DISCARDING,
            )
            | "GroupByKeyAndWindow" >> beam.GroupByKey(lambda element: element)
            | "CreateTimeSeries"
            >> beam.ParDo(
                CreateTimeSeries(self.job_name, self.project, self.region)
            )
        )
