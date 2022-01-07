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
"""Window and parse Pub/Sub streams of real-time weather data added to cloud storage.

Example windowing code borrowed from:
  https://cloud.google.com/pubsub/docs/pubsub-dataflow#code_sample
"""

import datetime
import fnmatch
import json
import logging
import random
import typing as t
from urllib.parse import urlparse

import apache_beam as beam
from apache_beam.transforms.window import FixedWindows

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class GroupMessagesByFixedWindows(beam.PTransform):
    """A composite transform that groups Pub/Sub messages based on publish time
    and outputs a list of tuples, each containing a message and its publish time.
    """

    def __init__(self, window_size: int, num_shards: int = 5):
        # Set window size to 60 seconds.
        self.window_size = int(window_size * 60)
        self.num_shards = num_shards

    def expand(self, pcoll):
        return (
                pcoll
                # Bind window info to each element using element timestamp (or publish time).
                | "Window into fixed intervals" >> beam.WindowInto(FixedWindows(self.window_size))
                | "Add timestamp to windowed elements" >> beam.ParDo(AddTimestamp())
                # Assign a random key to each windowed element based on the number of shards.
                | "Add key" >> beam.WithKeys(lambda _: random.randint(0, self.num_shards - 1))
                # Group windowed elements by key. All the elements in the same window must fit
                # memory for this. If not, you need to use `beam.util.BatchElements`.
                | "Group by key" >> beam.GroupByKey()
        )


class AddTimestamp(beam.DoFn):
    """Processes each windowed element by extracting the message body and its
    publish time into a tuple.
    """

    def process(self, element, publish_time=beam.DoFn.TimestampParam) -> t.Iterable[t.Tuple[str, str]]:
        yield (
            element.decode("utf-8"),
            datetime.datetime.utcfromtimestamp(float(publish_time)).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            ),
        )


class ParsePaths(beam.DoFn):
    """Parse paths to real-time weather data from windowed-batches."""

    def __init__(self, uri_pattern: str):
        self.uri_pattern = uri_pattern
        self.protocol = f'{urlparse(uri_pattern).scheme}://'
        super().__init__()

    @classmethod
    def try_parse_message(cls, message_body: t.Union[str, t.Dict]) -> t.Dict:
        """Robustly parse message body, which will be JSON in the vast majority of cases,
         but might be a dictionary."""
        try:
            return json.loads(message_body)
        except (json.JSONDecodeError, TypeError):
            if type(message_body) is dict:
                return message_body
            raise

    def to_object_path(self, payload: t.Dict) -> str:
        """Parse cloud object from Pub/Sub topic payload."""
        return f'{self.protocol}{payload["bucket"]}/{payload["name"]}'

    def should_skip(self, message_body: t.Dict) -> bool:
        """Returns true if Pub/Sub topic does *not* match the target file URI pattern."""
        try:
            return not fnmatch.fnmatch(self.to_object_path(message_body), self.uri_pattern)
        except KeyError:
            return True

    def process(self, key_value, window=beam.DoFn.WindowParam) -> t.Iterable[str]:
        """Yield paths to real-time weather data in cloud storage."""

        shard_id, batch = key_value

        logger.debug(f'Processing shard {shard_id!r}.')

        for message_body, publish_time in batch:
            logger.debug(message_body)

            parsed_msg = self.try_parse_message(message_body)

            target = self.to_object_path(parsed_msg)
            logger.info(f'Parsed path {target!r}...')

            if self.should_skip(parsed_msg):
                logger.info(f'skipping {target!r}.')
                continue

            yield target
