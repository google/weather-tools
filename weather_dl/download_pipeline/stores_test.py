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
import os
import tempfile
import unittest

from .stores import FSStore


class FSStoreTest(unittest.TestCase):

    def test_writes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target = f'{tmpdir}/my-file'
            with FSStore().open(target, 'wb') as f:
                f.write(b'{"key": 1}')

            self.assertTrue(os.path.exists(target))

    def test_reads(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target = f'{tmpdir}/my-file'
            with open(target, 'w') as f:
                f.write('data')
            with FSStore().open(target, 'rb') as f:
                self.assertEqual(f.readlines(), [b'data'])

    def test_reads__default_argument(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            target = f'{tmpdir}/my-file'
            with open(target, 'w') as f:
                f.write('data')
            with FSStore().open(target) as f:
                self.assertEqual(f.readlines(), [b'data'])

    def test_asserts_bad_mode__both(self):
        with self.assertRaisesRegex(ValueError, "invalid mode 'rw':"):
            with tempfile.TemporaryDirectory() as tmpdir:
                target = f'{tmpdir}/my-file'
                with FSStore().open(target, 'rw') as f:
                    f.read()

    def test_asserts_bad_mode__neither(self):
        with self.assertRaisesRegex(ValueError, "invalid mode '':"):
            with tempfile.TemporaryDirectory() as tmpdir:
                target = f'{tmpdir}/my-file'
                with FSStore().open(target, '') as f:
                    f.read()
