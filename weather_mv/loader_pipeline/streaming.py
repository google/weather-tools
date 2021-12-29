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

import datetime
import json
import logging
import random
import typing as t

import apache_beam as beam
from apache_beam.transforms.window import FixedWindows

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

GCS_PROTOCOL = 'gs://'


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


def to_object_path(payload: t.Dict) -> str:
    """Parse GCS object from Pub/Sub topic payload."""
    path = f'{GCS_PROTOCOL}{payload["bucket"]}/{payload["name"]}'
    logger.info(path)
    return path


def parse_message(message_body) -> t.Dict:
    """Robustly parse message body, which will be JSON in the vast majority of cases,
     but might be a dictionary."""
    try:
        return json.loads(message_body)
    except (json.JSONDecodeError, TypeError):
        if type(message_body) is dict:
            return message_body
        raise


def should_skip(message_body: t.Dict) -> bool:
    """Returns true if Pub/Sub topic payload is *not* a path to a realtime Grib file."""
    try:
        # TODO(alxr): Add more filtering conditions...
        return message_body['contentType'] != 'application/octet-stream'
    except KeyError:
        return True


class ParsePaths(beam.DoFn):
    """Parse paths to Grib files from windowed-batches."""

    def process(self, key_value, window=beam.DoFn.WindowParam) -> t.Iterable[str]:
        """Yield paths to Grib files in GCS."""

        shard_id, batch = key_value

        for message_body, publish_time in batch:
            logger.debug(message_body)

            parsed_msg = parse_message(message_body)

            target = to_object_path(parsed_msg)

            if should_skip(parsed_msg):
                logger.info(f'skipping.')
                continue

            yield target
