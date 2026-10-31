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
import os
import random
import string
import tempfile
import typing as t
import unittest

from .manifest import (
    LocalManifest,
    Location,
    DownloadStatus,
    ManifestException,
    PostgresManifest,
    Status,
    Stage,
)

POSTGRES_DSN = os.environ.get('POSTGRES_MANIFEST_DSN')
POSTGRES_TEST_TABLE = 'weather_tools_manifest_test'


def postgres_location(table: str) -> Location:
    separator = '&' if '?' in (POSTGRES_DSN or '') else '?'
    return Location(f'{POSTGRES_DSN}{separator}table={table}')


def rand_str(max_len=32):
    return ''.join([random.choice(string.printable) for _ in range(random.randint(0, max_len))])


def make_download_status(location: t.Optional[str] = None) -> DownloadStatus:
    return DownloadStatus(
        selection={},
        location=rand_str() if location is None else location,
        status=random.choice([Status.SCHEDULED, Status.IN_PROGRESS,
                              Status.SUCCESS, Status.FAILURE]),
        error=random.choice([None] + [rand_str(100) for _ in range(4)]),
        username=random.choice(['user', 'alice', 'bob', 'root']),
        stage=random.choice([Stage.FETCH, Stage.DOWNLOAD,
                             Stage.UPLOAD, Stage.RETRIEVE, None]),
        size=0.039322572,
        scheduled_time="2023-02-07T17:15:26+00:00",
        retrieve_start_time=None,
        retrieve_end_time=None,
        fetch_start_time="2023-02-07T17:15:29+00:00",
        fetch_end_time="2023-02-07T17:21:37+00:00",
        download_start_time="2023-02-07T17:21:39+00:00",
        download_end_time="2023-02-07T17:21:56+00:00",
        upload_start_time="2023-02-07T17:21:59+00:00",
        upload_end_time="2023-02-07T17:22:03+00:00"
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


class PostgresManifestLocationTest(unittest.TestCase):

    def test_parse_location__extracts_table(self):
        dsn, table = PostgresManifest.parse_location(
            Location('postgresql://user:pass@localhost:5432/weather?table=downloads'))

        self.assertEqual(dsn, 'postgresql://user:pass@localhost:5432/weather')
        self.assertEqual(table, 'downloads')

    def test_parse_location__defaults_table(self):
        dsn, table = PostgresManifest.parse_location(Location('postgresql://user:pass@localhost/weather'))

        self.assertEqual(dsn, 'postgresql://user:pass@localhost/weather')
        self.assertEqual(table, PostgresManifest.DEFAULT_TABLE)

    def test_parse_location__preserves_other_query_params(self):
        dsn, table = PostgresManifest.parse_location(
            Location('postgresql://user:pass@localhost/weather?table=downloads&sslmode=require'))

        self.assertEqual(table, 'downloads')
        self.assertIn('sslmode=require', dsn)
        self.assertNotIn('table=', dsn)

    def test_parse_location__rejects_invalid_table_name(self):
        with self.assertRaises(ManifestException):
            PostgresManifest.parse_location(Location('postgresql://localhost/weather?table=bad;drop'))

    def test_manifest_registry__supports_postgres_schemes(self):
        from .manifest import MANIFESTS

        self.assertIs(MANIFESTS['postgresql'], PostgresManifest)
        self.assertIs(MANIFESTS['postgres'], PostgresManifest)

    def test_to_db_value__normalizes_empty_timestamps(self):
        self.assertIsNone(PostgresManifest._to_db_value('scheduled_time', ''))
        self.assertIsNone(PostgresManifest._to_db_value('upload_end_time', None))
        self.assertEqual(
            PostgresManifest._to_db_value('scheduled_time', '2023-02-07T17:15:26+00:00'),
            '2023-02-07T17:15:26+00:00')

    def test_to_db_value__leaves_non_timestamps_untouched(self):
        self.assertEqual(PostgresManifest._to_db_value('username', ''), '')

    def test_to_serializable__converts_datetimes_to_isoformat(self):
        import datetime

        record = {
            'location': 'gs://bucket/file.nc',
            'scheduled_time': datetime.datetime(2023, 2, 7, 17, 15, 26, tzinfo=datetime.timezone.utc),
            'size': 0.5,
        }

        actual = PostgresManifest._to_serializable(record)

        self.assertEqual(actual['scheduled_time'], '2023-02-07T17:15:26+00:00')
        self.assertEqual(actual['location'], 'gs://bucket/file.nc')
        self.assertEqual(actual['size'], 0.5)


@unittest.skipUnless(POSTGRES_DSN, 'POSTGRES_MANIFEST_DSN is not set; skipping PostgreSQL tests.')
class PostgresManifestTest(unittest.TestCase):

    def setUp(self):
        self._drop_table()
        self.manifest = PostgresManifest(postgres_location(POSTGRES_TEST_TABLE))

    def tearDown(self):
        self._drop_table()

    def _drop_table(self):
        import psycopg2
        dsn, table = PostgresManifest.parse_location(postgres_location(POSTGRES_TEST_TABLE))
        with psycopg2.connect(dsn) as conn, conn.cursor() as cursor:
            cursor.execute(f'DROP TABLE IF EXISTS "{table}"')

    def test_read__missing_location_returns_empty_status(self):
        actual = self.manifest._read('gs://bucket/does-not-exist.nc')

        self.assertEqual(actual, DownloadStatus())

    def test_update__round_trips_download_status(self):
        expected = make_download_status(location='gs://bucket/file.nc')
        self.manifest._update(expected)

        actual = self.manifest._read('gs://bucket/file.nc')

        self.assertEqual(actual.location, expected.location)
        self.assertEqual(actual.status, expected.status)
        self.assertEqual(actual.stage, expected.stage)
        self.assertEqual(actual.username, expected.username)
        self.assertEqual(actual.size, expected.size)
        self.assertEqual(actual.scheduled_time, expected.scheduled_time)
        self.assertEqual(actual.download_start_time, expected.download_start_time)
        self.assertEqual(actual.upload_end_time, expected.upload_end_time)

    def test_update__overwrites_existing_status(self):
        location = 'gs://bucket/file.nc'
        self.manifest._update(make_download_status(location=location))
        expected = DownloadStatus(location=location, status=Status.FAILURE, error='boom', username='alice')
        self.manifest._update(expected)

        actual = self.manifest._read(location)

        self.assertEqual(actual.location, location)
        self.assertEqual(actual.status, Status.FAILURE)
        self.assertEqual(actual.error, 'boom')
        self.assertEqual(actual.username, 'alice')

    def test_update__keeps_one_row_per_location(self):
        import psycopg2
        location = 'gs://bucket/file.nc'
        for _ in range(5):
            self.manifest._update(make_download_status(location=location))

        dsn, table = PostgresManifest.parse_location(postgres_location(POSTGRES_TEST_TABLE))
        with psycopg2.connect(dsn) as conn, conn.cursor() as cursor:
            cursor.execute(f'SELECT COUNT(*) FROM "{table}" WHERE location = %s', (location,))
            count = cursor.fetchone()[0]

        self.assertEqual(count, 1)
