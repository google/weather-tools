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

import json
import random
import string
import tempfile
import time
import typing as t
import unittest

from .manifest import LocalManifest, Location, DownloadStatus


def rand_str(max_len=32):
    return ''.join([random.choice(string.printable) for _ in range(random.randint(0, max_len))])


def make_download_status(location: t.Optional[str] = None) -> DownloadStatus:
    return DownloadStatus(
        selection={},
        location=rand_str() if location is None else location,
        status=random.choice(['scheduled', 'in-progress', 'success', 'failure']),
        error=random.choice([None] + [rand_str(100) for _ in range(4)]),
        user=random.choice(['user', 'alice', 'bob', 'root']),
        download_finished_time=random.choice([None] + [int(time.time())]),
        download_scheduled_time=random.choice([None] + [int(time.time())]),
        download_duration=random.choice([None] + [random.randint(2, 137 ** 4)])
    )


class LocalManifestTest(unittest.TestCase):
    NUM_RUNS = 128

    def test_empty_manifest_is_valid_json(self):
        with tempfile.TemporaryDirectory() as dir_:
            manifest = LocalManifest(Location(dir_))
            with open(manifest.location) as file:
                self.is_valid_json(file)

    def does_not_overwrite_existing_manifest(self):
        with tempfile.TemporaryDirectory() as dir_:
            with open(f'{dir}/manifest.json', 'w') as file:
                json.dump({'foo': 'bar'}, file)

            manifest = LocalManifest(Location(dir_))

            with open(manifest.location, 'r') as file:
                manifest = json.load(file)
                self.assertIn('foo', manifest)
                self.assertEqual(manifest['foo'], 'bar')

    def test_writes_valid_json(self):
        with tempfile.TemporaryDirectory() as dir_:
            manifest = LocalManifest(Location(dir_))
            for _ in range(self.NUM_RUNS):
                status = make_download_status()
                manifest._update(status)
                with open(manifest.location) as file:
                    self.is_valid_json(file)

    def test_overwrites_existing_statuses(self):
        locations = ['a', 'b', 'c']
        with tempfile.TemporaryDirectory() as dir_:
            manifest = LocalManifest(Location(dir_))
            for i in range(self.NUM_RUNS):
                status = make_download_status(location=locations[i % 3])
                manifest._update(status)
                with open(manifest.location) as file:
                    self.is_valid_json(file)

            with open(manifest.location) as file:
                manifest = json.load(file)
            self.assertEqual(set(locations), set(manifest.keys()))

    def is_valid_json(self, file: t.IO) -> None:
        """Fails test on error decoding JSON."""
        try:
            json.dumps(json.load(file))
        except json.JSONDecodeError:
            self.fail('JSON is invalid.')
