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

import collections
import random
import string
import unittest

from .download_pipeline.manifest import FirestoreManifest

from weather_dl.download_status import main


class FakeCollectionReference:
    """A Firebase CollectionReference Fake for Tests"""

    def __init__(self, num_docs: int, prefix: str, status: str):
        self.num_docs = num_docs
        self.prefix = prefix
        self.status = status

    def stream(self):
        return [FakeDocumentReference(prefix=self.prefix, status=self.status) for _ in range(self.num_docs)]


class FakeDocumentReference:
    """A Firebase DocumentReference Fake for Tests"""

    def __init__(self, prefix: str = 'gs://', status: str = 'success'):
        self.prefix = prefix
        self.status = status

    def collections(self):
        return [FakeCollectionReference(i, prefix=self.prefix, status=self.status) for i in range(7)]

    def to_dict(self):
        return dict(
            location=self.prefix + ''.join([random.choice(string.printable) for _ in range(10)]),
            status=self.status,
        )


class FakeSuccessFirestoreManifest(FirestoreManifest):
    def root_document_for_store(self, store_scheme: str):
        return FakeDocumentReference()


class FakeInProgressFirestoreManifest(FirestoreManifest):
    def root_document_for_store(self, store_scheme: str):
        return FakeDocumentReference(status='in-progress')


class DownloadStatusTest(unittest.TestCase):

    def test_only_supports_firestore_manifests(self):
        with self.assertRaises(NotImplementedError) as e:
            main('gs://ecmwf-downloads/hres/ -m gs://some-other-manifest/foo'.split())

        self.assertEqual(e.exception.args[0], 'Only Firestore Manifests are supported!')

    def test_counts_statuses(self):
        fake_manifests = {'fs': FakeSuccessFirestoreManifest}
        expected = collections.Counter({'success': 21})
        actual = main('gs://'.split(), manifests=fake_manifests)
        self.assertEqual(expected, actual)

    def test_skips_counts_that_do_not_match_prefix(self):
        fake_manifests = {'fs': FakeInProgressFirestoreManifest}
        expected = collections.Counter()
        actual = main('gs://dont-match-bucket/foo/'.split(), manifests=fake_manifests)
        self.assertEqual(expected, actual)
